"""Central Bank Speeches Vector Search Dashboard.

Interactive dashboard for exploring central bank speeches using semantic search.
Supports switching between real and synthetic (privacy-preserving) data products.

Run with: marimo run dashboard.py
Edit with: marimo edit dashboard.py
"""

import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Central Bank Speeches Explorer

        Search through thousands of central bank speeches using AI-powered semantic search.
        Toggle between real and synthetic (privacy-preserving) data products.

        **Features:**
        - Vector similarity search using local NIM embedding model
        - Real-time search across ~10K central bank speeches
        - Toggle between real and synthetic data collections
        - Tariff mention classification via NIM LLM
        """
    )
    return


@app.cell
def __():
    from utils import (
        check_services,
        get_collection_stats,
        get_sample_queries,
        load_data_product,
        vector_search,
    )

    import polars as pl

    return (
        check_services,
        get_collection_stats,
        get_sample_queries,
        load_data_product,
        pl,
        vector_search,
    )


@app.cell
def __(check_services, mo):
    # Check service availability
    services = check_services()

    service_status = []
    for name, available in services.items():
        icon = "+" if available else "x"
        status = "Available" if available else "Unavailable"
        service_status.append(f"[{icon}] {name}: {status}")

    if all(services.values()):
        mo.md("**Services Status:** All services connected")
    else:
        mo.callout(
            "\n".join(service_status),
            kind="warn" if any(services.values()) else "danger",
        )
    return name, service_status, services, available, icon, status


@app.cell
def __(mo):
    mo.md("## Data Source")
    return


@app.cell
def __(mo):
    use_synthetic = mo.ui.switch(label="Use Synthetic Data", value=False)
    use_synthetic
    return (use_synthetic,)


@app.cell
def __(use_synthetic):
    collection = "SyntheticSpeeches" if use_synthetic.value else "CentralBankSpeeches"
    return (collection,)


@app.cell
def __(collection, get_collection_stats, mo):
    stats = get_collection_stats(collection)

    if stats["exists"]:
        mo.md(
            f"""
            **Active Collection**: `{collection}`
            **Total Speeches**: {stats['count']:,}
            """
        )
    else:
        mo.callout(
            f"Collection '{collection}' not found. Run the Dagster pipeline first to index speeches.",
            kind="danger",
        )
    return (stats,)


@app.cell
def __(mo):
    mo.md("## Search")
    return


@app.cell
def __(get_sample_queries, mo):
    sample_queries = get_sample_queries()

    # Create form using batch().form() pattern
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
                value=sample_queries[0],
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

    mo.md(f"_Sample queries: {', '.join(sample_queries[:5])}..._")
    search_form
    return sample_queries, search_form


@app.cell
def __(mo):
    show_text = mo.ui.switch(label="Show Text Preview", value=False)
    show_text
    return (show_text,)


@app.cell
def __(collection, mo, search_form, vector_search):
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
def __(mo, results, show_text):
    output_parts = []
    if results:
        for i, result in enumerate(results, 1):
            similarity = result.get("_similarity", 0) * 100

            card_content = f"""
**{i}. {result.get('title', 'Untitled')}**

| Field | Value |
|-------|-------|
| Central Bank | {result.get('central_bank', 'Unknown')} |
| Speaker | {result.get('speaker', 'Unknown')} |
| Date | {result.get('date', 'Unknown')} |
| Tariff Mention | {'Yes' if result.get('tariff_mention') else 'No'} |
| Similarity | {similarity:.1f}% |
"""
            if show_text.value:
                text = (result.get("text", "") or "")[:500]
                card_content += f"\n**Text Preview:**\n\n_{text}..._"

            output_parts.append(card_content)
            output_parts.append("---")

        mo.md("\n".join(output_parts))
    return (output_parts,)


@app.cell
def __(mo):
    mo.md("## Data Product Overview")
    return


@app.cell
def __(load_data_product, mo, pl, use_synthetic):
    try:
        df = load_data_product(use_synthetic=use_synthetic.value)

        # Summary statistics
        summary_data = {
            "Metric": [
                "Total Speeches",
                "Central Banks",
                "Speakers",
                "Tariff Mentions",
                "Date Range",
            ],
            "Value": [
                str(len(df)),
                str(df["central_bank"].n_unique()),
                str(df["speaker"].n_unique()) if "speaker" in df.columns else "N/A",
                str(df.filter(pl.col("tariff_mention") == 1).height),
                f"{df['date'].min()} to {df['date'].max()}" if "date" in df.columns else "N/A",
            ],
        }

        mo.ui.table(pl.DataFrame(summary_data).to_pandas(), selection=None)
    except Exception as e:
        mo.callout(f"Could not load data product: {e}", kind="warn")
        df = None
    return df, summary_data


@app.cell
def __(df, mo, pl):
    if df is not None:
        mo.md("### Central Banks by Speech Count")

        bank_counts = (
            df.group_by("central_bank")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(15)
        )

        mo.ui.table(bank_counts.to_pandas(), selection=None)
    return (bank_counts,)


@app.cell
def __(mo):
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
