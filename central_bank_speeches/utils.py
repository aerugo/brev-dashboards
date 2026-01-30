"""Utility functions for Central Bank Speeches dashboard."""

import io
import os
from typing import Any

import polars as pl
import requests
import weaviate
from weaviate.classes.query import MetadataQuery

# Data product definitions
# Maps product key to collection name, LakeFS path, and display info
DATA_PRODUCTS = {
    "full_real": {
        "label": "Full Real Data",
        "description": "Complete real speeches dataset",
        "collection": "CentralBankSpeeches",
        "lakefs_path": "central-bank-speeches/enriched_speeches.parquet",
    },
    "full_synthetic": {
        "label": "Full Synthetic Data",
        "description": "Privacy-preserving synthetic speeches",
        "collection": "SyntheticSpeeches",
        "lakefs_path": "central-bank-speeches/synthetic/speeches.parquet",
    },
    "trial_real": {
        "label": "Trial Real Data",
        "description": "Sample of real speeches (10 records)",
        "collection": "CentralBankSpeechesTrial",
        "lakefs_path": "central-bank-speeches/trial/enriched_speeches.parquet",
    },
    "trial_synthetic": {
        "label": "Trial Synthetic Data",
        "description": "Sample of synthetic speeches (10 records)",
        "collection": "SyntheticSpeechesTrial",
        "lakefs_path": "central-bank-speeches/synthetic/trial/speeches.parquet",
    },
}

# Weaviate connection settings
WEAVIATE_HOST = os.getenv("WEAVIATE_HOST", "weaviate.weaviate.svc.cluster.local")
WEAVIATE_PORT = int(os.getenv("WEAVIATE_PORT", "80"))
WEAVIATE_GRPC_HOST = os.getenv("WEAVIATE_GRPC_HOST", "weaviate-grpc.weaviate.svc.cluster.local")
WEAVIATE_GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))

# NIM Embedding settings
NIM_EMBEDDING_ENDPOINT = os.getenv(
    "NIM_EMBEDDING_ENDPOINT",
    "http://nvidia-nim-embedding.nvidia-nim.svc.cluster.local:8000",
)
EMBEDDING_MODEL = "nvidia/llama-3_2-nemoretriever-300m-embed-v2"

# LakeFS settings
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs.lakefs.svc.cluster.local:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY_ID", "")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "")


def get_weaviate_client() -> weaviate.WeaviateClient:
    """Get a connected Weaviate client."""
    return weaviate.connect_to_custom(
        http_host=WEAVIATE_HOST,
        http_port=WEAVIATE_PORT,
        http_secure=False,
        grpc_host=WEAVIATE_GRPC_HOST,
        grpc_port=WEAVIATE_GRPC_PORT,
        grpc_secure=False,
    )


def embed_query(query: str) -> list[float]:
    """Generate embedding for a search query using local NIM.

    Args:
        query: Search query text.

    Returns:
        1024-dimensional embedding vector.

    Raises:
        requests.RequestException: If NIM embedding service is unavailable.
    """
    payload = {
        "model": EMBEDDING_MODEL,
        "input": [query],
        "input_type": "query",  # Use "query" type for search queries
        "encoding_format": "float",
    }

    response = requests.post(
        f"{NIM_EMBEDDING_ENDPOINT}/v1/embeddings",
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()["data"][0]["embedding"]


def vector_search(
    query: str,
    collection: str = "CentralBankSpeeches",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Perform vector similarity search on speeches.

    Args:
        query: Search query text.
        collection: Weaviate collection name.
        limit: Maximum results to return.

    Returns:
        List of matching speeches with similarity scores.
    """
    # Generate query embedding
    query_vector = embed_query(query)

    # Search in Weaviate
    client = get_weaviate_client()
    try:
        coll = client.collections.get(collection)

        results = coll.query.near_vector(
            near_vector=query_vector,
            limit=limit,
            return_metadata=MetadataQuery(distance=True, certainty=True),
        )

        output = []
        for obj in results.objects:
            item = dict(obj.properties)
            item["_distance"] = obj.metadata.distance
            item["_certainty"] = obj.metadata.certainty
            # Convert distance to similarity (cosine distance: 0 = identical)
            item["_similarity"] = 1 - (obj.metadata.distance or 0)
            output.append(normalize_columns(item))

        return output
    finally:
        client.close()


def get_collection_stats(collection: str) -> dict[str, Any]:
    """Get statistics about a Weaviate collection.

    Args:
        collection: Weaviate collection name.

    Returns:
        Dictionary with exists and count fields.
    """
    client = get_weaviate_client()
    try:
        if not client.collections.exists(collection):
            return {"exists": False, "count": 0}

        coll = client.collections.get(collection)
        response = coll.aggregate.over_all(total_count=True)

        return {
            "exists": True,
            "count": response.total_count or 0,
        }
    finally:
        client.close()


def load_data_product(use_synthetic: bool = False) -> pl.DataFrame:
    """Load the central bank speeches data product from LakeFS.

    Args:
        use_synthetic: If True, load synthetic data instead of real.

    Returns:
        Polars DataFrame with speech data.

    Raises:
        Exception: If data product cannot be loaded.
    """
    import lakefs_sdk
    from lakefs_sdk.client import LakeFSClient

    configuration = lakefs_sdk.Configuration(
        host=LAKEFS_ENDPOINT,
        username=LAKEFS_ACCESS_KEY,
        password=LAKEFS_SECRET_KEY,
    )

    client = LakeFSClient(configuration)

    # Determine path based on data source
    if use_synthetic:
        path = "central-bank-speeches/synthetic/speeches.parquet"
    else:
        path = "central-bank-speeches/speeches.parquet"

    # Download file from LakeFS
    response = client.objects_api.get_object(
        repository="data",
        ref="main",
        path=path,
    )

    return normalize_columns(pl.read_parquet(io.BytesIO(response.read())))


def normalize_columns(data: dict[str, Any] | pl.DataFrame) -> dict[str, Any] | pl.DataFrame:
    """Normalize column names across real and synthetic data products.

    Real Weaviate schema uses: central_bank, speaker, is_governor
    Synthetic schema uses: country, author, is_gov
    Enriched parquet uses: country, author, is_gov

    This function maps everything to a canonical set:
    central_bank/country -> country, speaker/author -> author, is_governor/is_gov -> is_gov
    """
    RENAMES = {
        "central_bank": "country",
        "speaker": "author",
        "is_governor": "is_gov",
    }
    if isinstance(data, pl.DataFrame):
        rename_map = {old: new for old, new in RENAMES.items() if old in data.columns and new not in data.columns}
        if rename_map:
            data = data.rename(rename_map)
        return data
    if isinstance(data, dict):
        for old, new in RENAMES.items():
            if old in data and new not in data:
                data[new] = data.pop(old)
        return data
    return data


def get_sample_queries() -> list[str]:
    """Return sample queries for the dashboard."""
    return [
        "inflation expectations and monetary policy",
        "trade tensions and tariffs impact",
        "interest rate decisions",
        "quantitative easing programs",
        "financial stability risks",
        "cryptocurrency and digital currencies",
        "climate change economic impact",
        "labor market conditions",
        "supply chain disruptions",
        "housing market trends",
    ]


def check_services() -> dict[str, bool]:
    """Check availability of required services.

    Returns:
        Dictionary with service status (weaviate, nim_embedding, lakefs).
    """
    status = {}

    # Check Weaviate
    try:
        client = get_weaviate_client()
        client.is_ready()
        status["weaviate"] = True
        client.close()
    except Exception:
        status["weaviate"] = False

    # Check NIM Embedding
    try:
        response = requests.get(f"{NIM_EMBEDDING_ENDPOINT}/v1/health/ready", timeout=5)
        status["nim_embedding"] = response.status_code == 200
    except Exception:
        status["nim_embedding"] = False

    # Check LakeFS
    try:
        response = requests.get(f"{LAKEFS_ENDPOINT}/api/v1/healthcheck", timeout=5)
        status["lakefs"] = response.status_code == 204 or response.status_code == 200
    except Exception:
        status["lakefs"] = False

    return status


def get_available_data_products() -> dict[str, dict[str, Any]]:
    """Detect which data products have been materialized.

    Checks both Weaviate collections and LakeFS paths to determine
    which data products are available for use.

    Returns:
        Dictionary of available products with their metadata and record counts.
    """
    import lakefs_sdk
    from lakefs_sdk.client import LakeFSClient

    available = {}

    # Get Weaviate client for collection checks
    try:
        weaviate_client = get_weaviate_client()
        weaviate_available = True
    except Exception:
        weaviate_client = None
        weaviate_available = False

    # Get LakeFS client for path checks
    try:
        configuration = lakefs_sdk.Configuration(
            host=LAKEFS_ENDPOINT,
            username=LAKEFS_ACCESS_KEY,
            password=LAKEFS_SECRET_KEY,
        )
        lakefs_client = LakeFSClient(configuration)
        lakefs_available = True
    except Exception:
        lakefs_client = None
        lakefs_available = False

    for product_key, product_info in DATA_PRODUCTS.items():
        collection_name = product_info["collection"]
        lakefs_path = product_info["lakefs_path"]

        # Check Weaviate collection
        weaviate_count = 0
        weaviate_exists = False
        if weaviate_available and weaviate_client:
            try:
                if weaviate_client.collections.exists(collection_name):
                    coll = weaviate_client.collections.get(collection_name)
                    response = coll.aggregate.over_all(total_count=True)
                    weaviate_count = response.total_count or 0
                    weaviate_exists = weaviate_count > 0
            except Exception:
                pass

        # Check LakeFS path
        lakefs_exists = False
        if lakefs_available and lakefs_client:
            try:
                lakefs_client.objects_api.stat_object(
                    repository="data",
                    ref="main",
                    path=lakefs_path,
                )
                lakefs_exists = True
            except Exception:
                pass

        # Product is available if it has data in Weaviate (for search)
        if weaviate_exists:
            available[product_key] = {
                **product_info,
                "weaviate_count": weaviate_count,
                "lakefs_exists": lakefs_exists,
            }

    # Close Weaviate client
    if weaviate_client:
        try:
            weaviate_client.close()
        except Exception:
            pass

    return available


def load_data_product_by_key(product_key: str) -> pl.DataFrame:
    """Load a data product by its key.

    Args:
        product_key: Key from DATA_PRODUCTS (e.g., 'full_real', 'trial_synthetic').

    Returns:
        Polars DataFrame with speech data.

    Raises:
        ValueError: If product_key is not valid.
        Exception: If data product cannot be loaded.
    """
    if product_key not in DATA_PRODUCTS:
        raise ValueError(f"Unknown data product: {product_key}")

    import lakefs_sdk
    from lakefs_sdk.client import LakeFSClient

    configuration = lakefs_sdk.Configuration(
        host=LAKEFS_ENDPOINT,
        username=LAKEFS_ACCESS_KEY,
        password=LAKEFS_SECRET_KEY,
    )

    client = LakeFSClient(configuration)
    path = DATA_PRODUCTS[product_key]["lakefs_path"]

    response = client.objects_api.get_object(
        repository="data",
        ref="main",
        path=path,
    )

    return normalize_columns(pl.read_parquet(io.BytesIO(response.read())))
