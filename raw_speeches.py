# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo>=0.19.4",
#     "polars>=1.0.0",
#     "lakefs-sdk>=1.0.0",
#     "numpy",
#     "pyarrow",
# ]
# ///
import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import io
    import polars as pl
    from lakefs_sdk import Configuration, ApiClient
    from lakefs_sdk.api import ObjectsApi

    endpoint = os.environ["LAKEFS_ENDPOINT"] + "/api/v1"

    # LakeFS connection
    config = Configuration(host=endpoint)
    config.username = os.environ["LAKEFS_ACCESS_KEY_ID"]
    config.password = os.environ["LAKEFS_SECRET_ACCESS_KEY"]

    client = ApiClient(config)
    objects_api = ObjectsApi(client)

    # Read the parquet file from LakeFS
    response = objects_api.get_object(
      repository="data",
      ref="main",
      path="central-bank-speeches/raw_speeches.parquet"
    )

    # Load into Polars
    df = pl.read_parquet(io.BytesIO(response))
    df
    return


if __name__ == "__main__":
    app.run()
