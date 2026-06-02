"""
ARCAS - src/arcas_dashboard/pages/network_explorer.py
Interactive knowledge graph visualisation using PyVis.
"""
import streamlit as st
import httpx
from pyvis.network import Network
import tempfile, os

API_BASE = "http://localhost:8000/api/v1"


def render():
    st.title("🕸️ Network Explorer")
    st.caption("Interactive visualisation of the actor relationship graph.")

    actor_id = st.text_input("Actor Token (pseudonymous)", placeholder="Enter actor_id to explore")
    depth    = st.slider("Network depth", 1, 3, 2)

    if st.button("Explore Network") and actor_id:
        try:
            resp = httpx.get(f"{API_BASE}/actors/{actor_id}/network", params={"depth": depth}, timeout=10.0)
            if resp.status_code == 200:
                data  = resp.json()
                nodes = data.get("nodes", [])
                edges = data.get("edges", [])
                net   = Network(height="500px", width="100%", bgcolor="#0e1117", font_color="white")
                for actor_id, name, atype in nodes:
                    net.add_node(actor_id, label=name or actor_id[:8], title=atype)
                for edge in edges:
                    net.add_edge(edge.get("source", ""), edge.get("target", ""),
                                 label=edge.get("type", ""), value=edge.get("strength", 1.0))
                with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
                    net.save_graph(f.name)
                    html = open(f.name).read()
                    os.unlink(f.name)
                st.components.v1.html(html, height=520)
            else:
                st.warning("Actor not found or API unavailable.")
        except Exception as e:
            st.error(f"Network query failed: {e}")
    else:
        st.info("Enter an actor token above and click Explore Network.")
