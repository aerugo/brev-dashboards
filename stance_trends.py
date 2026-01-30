# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo>=0.19.4",
#     "polars>=1.0.0",
#     "lakefs-sdk>=1.0.0",
#     "altair>=5.0.0",
#     "pandas",
#     "numpy",
#     "pyarrow",
# ]
# ///
"""Stance Trends Dashboard.

Monthly averages of monetary_stance, trade_stance, and economic_outlook
for real vs synthetic central bank speeches, with country filtering.

Run with: marimo run stance_trends.py
Edit with: marimo edit stance_trends.py
"""

import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os
    import io

    import polars as pl
    from lakefs_sdk import Configuration, ApiClient
    from lakefs_sdk.api import ObjectsApi

    _endpoint = os.environ["LAKEFS_ENDPOINT"] + "/api/v1"
    _config = Configuration(host=_endpoint)
    _config.username = os.environ["LAKEFS_ACCESS_KEY_ID"]
    _config.password = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    _client = ApiClient(_config)
    objects_api = ObjectsApi(_client)

    return io, objects_api, pl


@app.cell
def _(io, mo, objects_api, pl):
    _ALL_DATASETS = {
        "Real": "central-bank-speeches/enriched_speeches.parquet",
        "Synthetic": "central-bank-speeches/synthetic/speeches.parquet",
    }

    # Only offer datasets that exist in LakeFS
    _available = {}
    for _name, _path in _ALL_DATASETS.items():
        try:
            objects_api.stat_object(repository="data", ref="main", path=_path)
            _available[_name] = _path
        except Exception:
            pass

    DATASETS = _available

    if not DATASETS:
        dataset_dropdown = None
    else:
        dataset_dropdown = mo.ui.dropdown(
            options=list(DATASETS.keys()),
            value=list(DATASETS.keys())[0],
            label="Dataset",
        )

    def _load(name):
        _path = DATASETS[name]
        _resp = objects_api.get_object(repository="data", ref="main", path=_path)
        _df = pl.read_parquet(io.BytesIO(_resp))
        _renames = {"central_bank": "country", "speaker": "author", "is_governor": "is_gov"}
        _rename_map = {old: new for old, new in _renames.items() if old in _df.columns and new not in _df.columns}
        if _rename_map:
            _df = _df.rename(_rename_map)
        return _df

    load_dataset = _load

    # Top-level output: show dropdown or error
    dataset_dropdown if dataset_dropdown is not None else mo.callout("No datasets found in LakeFS.", kind="danger")

    return DATASETS, dataset_dropdown, load_dataset


@app.cell
def _(dataset_dropdown, load_dataset, mo, pl):
    mo.stop(dataset_dropdown is None, mo.md("_No dataset available._"))
    df = load_dataset(dataset_dropdown.value)
    return (df,)


@app.cell
def _(df, mo):
    _countries = sorted(df["country"].unique().to_list()) if "country" in df.columns else []

    country_filter = mo.ui.multiselect(
        options=_countries,
        label="Filter by Country",
    )
    country_filter
    return (country_filter,)


@app.cell
def _(country_filter, df, mo, pl):
    import altair as alt

    _df = df
    if country_filter.value:
        _df = _df.filter(pl.col("country").is_in(country_filter.value))

    _stance_cols = [c for c in ["monetary_stance", "trade_stance", "economic_outlook"] if c in _df.columns]

    mo.stop(
        not _stance_cols or "date" not in _df.columns,
        mo.md("_Required columns (date, monetary_stance, trade_stance, economic_outlook) not found._"),
    )

    # Parse date and extract month
    _df = _df.with_columns(pl.col("date").cast(pl.Date).alias("_date"))
    _df = _df.with_columns(pl.col("_date").dt.truncate("1mo").alias("month"))

    # Cast stance columns to float for averaging
    for _c in _stance_cols:
        _df = _df.with_columns(pl.col(_c).cast(pl.Float64))

    # Monthly averages
    _monthly = (
        _df.group_by("month")
        .agg([pl.col(c).mean().alias(c) for c in _stance_cols])
        .sort("month")
        .drop_nulls("month")
    )

    # Build Altair chart
    _long = _monthly.unpivot(
        index="month",
        on=_stance_cols,
        variable_name="metric",
        value_name="score",
    ).to_pandas()

    _chart = (
        alt.Chart(_long)
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("score:Q", title="Average Score (1-5)", scale=alt.Scale(domain=[1, 5])),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=["month:T", "metric:N", alt.Tooltip("score:Q", format=".2f")],
        )
        .properties(width=700, height=400, title="Monthly Stance Averages")
        .interactive()
    )

    mo.ui.altair_chart(_chart)
    return


if __name__ == "__main__":
    app.run()
