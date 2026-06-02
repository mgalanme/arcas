"""
ARCAS - src/arcas_dashboard/app.py

Streamlit HITL demo application.
Multi-page app: Alert Queue, Network Explorer, Pipeline Status,
Investigation Register, Governance, Run Daily Pipeline.
"""
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="ARCAS - Anti-Corruption & Accountability System",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation
st.sidebar.title("🔍 ARCAS")
st.sidebar.caption("Anti-Corruption & Accountability System")
st.sidebar.divider()

pages = {
    "🏠 Overview":              "pages/overview",
    "🚨 Alert Queue (HITL)":    "pages/alert_queue",
    "🕸️ Network Explorer":       "pages/network_explorer",
    "📊 Pipeline Status":        "pages/pipeline_status",
    "📋 Investigation Register": "pages/investigations",
    "⚖️ Judicial Patterns":      "pages/judicial",
    "📰 Disinformation Tracker": "pages/disinfo",
    "🔒 Governance":             "pages/governance",
    "▶️  Run Daily Pipeline":    "pages/run_pipeline",
}

selected = st.sidebar.radio("Navigate", list(pages.keys()))
st.sidebar.divider()
st.sidebar.caption("v1.0.0 | Educational Project")
st.sidebar.caption("Not for production use")

# Route to selected page
if selected == "🏠 Overview":
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pending Alerts", "0", help="Alerts awaiting HITL review")
    with col2:
        st.metric("Validated Today", "0", help="Alerts validated by operators today")
    with col3:
        st.metric("Actors Tracked", "0", help="Unique actors in knowledge graph")
    with col4:
        st.metric("Sources Active", "0", help="Ingestion sources currently active")
    st.info("Environment operational. Start the daily pipeline to begin ingestion.")

elif selected == "🚨 Alert Queue (HITL)":
    from pages.alert_queue import render
    render()

elif selected == "🕸️ Network Explorer":
    from pages.network_explorer import render
    render()

elif selected == "📊 Pipeline Status":
    from pages.pipeline_status import render
    render()

elif selected == "▶️  Run Daily Pipeline":
    from pages.run_pipeline import render
    render()

else:
    st.title(selected)
    st.info("This page is under construction. Coming in Phase 2.")
