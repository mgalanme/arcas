"""
ARCAS - src/arcas_dashboard/pages/run_pipeline.py
Manual trigger for the daily data collection and analysis cycle.
"""
import time
import streamlit as st
from datetime import date


def render():
    st.title("▶️ Run Daily Pipeline")
    st.caption("Manually trigger the full daily ingestion and analysis cycle.")
    st.warning("This will ingest today's public data and run all detection agents. Estimated time: 5-15 minutes.")

    run_date = st.date_input("Target date", value=date.today())
    sources  = st.multiselect(
        "Sources to ingest",
        ["BOE (Official Gazette)", "PLACSP (Procurement)", "Media Scraper"],
        default=["BOE (Official Gazette)", "PLACSP (Procurement)"],
    )

    if st.button("🚀 Start Pipeline", type="primary"):
        progress = st.progress(0)
        status   = st.empty()

        steps = [
            (10,  "Fetching BOE publications..."),
            (25,  "Fetching PLACSP contract data..."),
            (40,  "Running NLP pipeline (NER + embeddings)..."),
            (55,  "Updating knowledge graph..."),
            (70,  "Running detection agents (Categories A-F)..."),
            (85,  "Generating draft alerts..."),
            (95,  "Exporting to Iceberg lakehouse..."),
            (100, "Pipeline complete."),
        ]

        for pct, msg in steps:
            status.info(f"Step {pct}%: {msg}")
            progress.progress(pct)
            time.sleep(0.5)  # Stub: replace with real pipeline calls

        st.success("Daily pipeline completed. Check the Alert Queue for new items.")
        st.balloons()
