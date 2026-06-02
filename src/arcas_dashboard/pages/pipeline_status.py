"""
ARCAS - src/arcas_dashboard/pages/pipeline_status.py
Real-time ingestion pipeline status page.
"""
import streamlit as st
import httpx

API_BASE = "http://localhost:8000/api/v1"

def render():
    st.title("📊 Pipeline Status")
    st.caption("Real-time ingestion and processing metrics.")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("BOE Records Today",        "0")
        st.metric("Procurement Records Today", "0")
    with col2:
        st.metric("NLP Pipeline Throughput",  "0 rec/min")
        st.metric("Knowledge Graph Nodes",    "0")
    with col3:
        st.metric("Kafka Consumer Lag",       "0")
        st.metric("Qdrant Vectors",           "0")

    st.info("Start the ingestion pipeline with: make ingest-test")
    st.divider()
    st.subheader("Kafka Topics")
    st.code("""
arcas.raw.gazette        - BOE raw publications
arcas.raw.procurement    - PLACSP contracts
arcas.normalised         - Deduplicated, normalised
arcas.nlp.extracted      - NLP-enriched records
arcas.processed          - Pseudonymised, ready for agents
arcas.alerts.draft       - Draft alerts pending HITL
arcas.hitl.requests      - Operator review requests
    """)
