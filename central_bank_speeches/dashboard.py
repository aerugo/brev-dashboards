# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo>=0.19.4",
#     "polars>=1.0.0",
#     "weaviate-client>=4.9.0",
#     "lakefs-sdk>=1.0.0",
#     "requests>=2.31.0",
#     "pandas",
# ]
# ///
"""Central Bank Speeches Vector Search Dashboard.

Interactive dashboard for exploring central bank speeches using semantic search.
Supports multiple data products: full/trial versions of real and synthetic data.
Only shows data products that have been materialized by the Dagster pipeline.

Run with: marimo run dashboard.py
Edit with: marimo edit dashboard.py
"""

import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Central Bank Speeches Explorer

        Search through thousands of central bank speeches using AI-powered semantic search.
        Choose from available data products including full and trial versions of real
        and synthetic (privacy-preserving) data.

        **Features:**
        - Vector similarity search using local NIM embedding model
        - Multiple data sources: full/trial real and synthetic datasets
        - Only shows data products that have been materialized
        - Tariff mention classification via NIM LLM
        """
    )
    return


@app.cell
def _():
    from utils import (
        DATA_PRODUCTS,
        check_services,
        get_available_data_products,
        get_collection_stats,
        get_sample_queries,
        load_data_product_by_key,
        vector_search,
    )

    import polars as pl

    return (
        DATA_PRODUCTS,
        check_services,
        get_available_data_products,
        get_collection_stats,
        get_sample_queries,
        load_data_product_by_key,
        pl,
        vector_search,
    )


@app.cell
def _(check_services, mo):
    services = check_services()

    _service_status = []
    for _name, _available in services.items():
        _icon = "+" if _available else "x"
        _status = "Available" if _available else "Unavailable"
        _service_status.append(f"[{_icon}] {_name}: {_status}")

    if all(services.values()):
        mo.md("**Services Status:** All services connected")
    else:
        mo.callout(
            "\n".join(_service_status),
            kind="warn" if any(services.values()) else "danger",
        )
    return (services,)


@app.cell
def _(get_available_data_products, mo):
    available_products = get_available_data_products()

    if not available_products:
        mo.callout(
            "No data products found. Run the Dagster pipeline first to index speeches.",
            kind="danger",
        )
        data_source = None
    else:
        _dropdown_options = {
            f"{info['label']} ({info['weaviate_count']:,} speeches)": key
            for key, info in available_products.items()
        }

        _default_label = list(_dropdown_options.keys())[0]

        data_source = mo.ui.dropdown(
            options=_dropdown_options,
            value=_default_label,
            label="Select Data Source",
        )
        data_source

    return available_products, data_source


@app.cell
def _(available_products, data_source):
    if data_source is not None and data_source.value:
        selected_product = available_products.get(data_source.value, {})
        collection = selected_product.get("collection", "CentralBankSpeeches")
        selected_product_key = data_source.value
    else:
        collection = None
        selected_product_key = None
        selected_product = {}
    return collection, selected_product, selected_product_key


@app.cell
def _(available_products, collection, data_source, mo):
    if data_source is None or collection is None:
        mo.md("_No data source selected._")
    else:
        _product_info = available_products.get(data_source.value, {})
        _lakefs_status = "Available" if _product_info.get("lakefs_exists") else "Not in LakeFS"

        mo.md(
            f"""
            **Active Collection**: `{collection}`

            **Total Speeches**: {_product_info.get('weaviate_count', 0):,}

            **Description**: {_product_info.get('description', 'N/A')}

            **LakeFS Data**: {_lakefs_status}
            """
        )
    return


@app.cell
def _(get_sample_queries, mo):
    _sample_queries = get_sample_queries()

    search_form = (
        mo.md(
            """
            **Search Query**

            {query}

            **Number of Results:** {num_results}
            """
        )
        .batch(
            query=mo.ui.text_area(
                placeholder="Enter your search query (e.g., 'inflation expectations')...",
                value=_sample_queries[0],
                full_width=True,
            ),
            num_results=mo.ui.slider(
                start=5,
                stop=50,
                value=10,
                step=5,
            ),
        )
        .form(submit_button_label="Search")
    )

    mo.vstack([
        mo.md(f"_Sample queries: {', '.join(_sample_queries[:5])}..._"),
        search_form,
    ])
    return (search_form,)


@app.cell
def _(mo):
    show_text = mo.ui.switch(label="Show Text Preview", value=False)
    show_text
    return (show_text,)


@app.cell
def _(collection, mo, search_form, vector_search):
    results = []

    if search_form.value is None:
        mo.md("_Enter a query and click 'Search' to find speeches._")
    elif not search_form.value["query"].strip():
        mo.callout("Please enter a search query.", kind="info")
    else:
        try:
            results = vector_search(
                query=search_form.value["query"],
                collection=collection,
                limit=search_form.value["num_results"],
            )
            mo.md(f"### Results ({len(results)} found)")
        except Exception as e:
            mo.callout(f"Search error: {e}", kind="danger")
    return (results,)


@app.cell
def _(mo, results, show_text):
    _output_parts = []
    if results:
        for _i, _result in enumerate(results, 1):
            _similarity = _result.get("_similarity", 0) * 100

            _card_content = f"""
**{_i}. {_result.get('title', 'Untitled')}**

| Field | Value |
|-------|-------|
| Country | {_result.get('country', 'Unknown')} |
| Author | {_result.get('author', 'Unknown')} |
| Date | {_result.get('date', 'Unknown')} |
| Tariff Mention | {'Yes' if _result.get('tariff_mention') else 'No'} |
| Similarity | {_similarity:.1f}% |
"""
            if show_text.value:
                _text = (_result.get("text") or _result.get("summary") or "")[:500]
                _card_content += f"\n**Text Preview:**\n\n_{_text}..._"

            _output_parts.append(_card_content)
            _output_parts.append("---")

        mo.md("\n".join(_output_parts))
    return


@app.cell
def _(mo):
    mo.md("## Data Product Overview")
    return


@app.cell
def _(load_data_product_by_key, mo, pl, selected_product, selected_product_key):
    df = None

    if selected_product_key is None:
        mo.md("_Select a data source to view statistics._")
    elif not selected_product.get("lakefs_exists"):
        mo.callout(
            f"Data product '{selected_product.get('label')}' is indexed in Weaviate but not available in LakeFS. "
            "Statistics are unavailable.",
            kind="warn",
        )
    else:
        try:
            df = load_data_product_by_key(selected_product_key)

            _summary_data = {
                "Metric": [
                    "Total Speeches",
                    "Countries",
                    "Authors",
                    "Tariff Mentions",
                    "Date Range",
                ],
                "Value": [
                    str(len(df)),
                    str(df["country"].n_unique()) if "country" in df.columns else "N/A",
                    str(df["author"].n_unique()) if "author" in df.columns else "N/A",
                    str(df.filter(pl.col("tariff_mention") == 1).height) if "tariff_mention" in df.columns else "N/A",
                    f"{df['date'].min()} to {df['date'].max()}" if "date" in df.columns else "N/A",
                ],
            }

            mo.ui.table(pl.DataFrame(_summary_data).to_pandas(), selection=None)
        except Exception as e:
            mo.callout(f"Could not load data product: {e}", kind="warn")

    return (df,)


@app.cell
def _(df, mo, pl):
    if df is not None and "country" in df.columns:
        mo.md("### Countries by Speech Count")

        _bank_counts = (
            df.group_by("country")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(15)
        )

        mo.ui.table(_bank_counts.to_pandas(), selection=None)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---

        **Central Bank Speeches Explorer** | Built with [Marimo](https://marimo.io)

        Data pipeline powered by Dagster, NVIDIA NIM, and Weaviate | Part of the Brev Data Platform
        """
    )
    return


if __name__ == "__main__":
    app.run()
