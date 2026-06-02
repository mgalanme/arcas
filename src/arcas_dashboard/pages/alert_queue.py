"""
ARCAS - src/arcas_dashboard/pages/alert_queue.py

HITL Alert Queue page.
Displays pending alerts with full reasoning chain and 9-action decision panel.
"""
import json
import httpx
import streamlit as st

API_BASE = "http://localhost:8000/api/v1"


def render():
    st.title("🚨 Alert Queue - Human Review")
    st.caption("Review AI-generated alerts and take decisions. Every action is audit-logged.")
    st.divider()

    # Fetch pending alerts
    try:
        resp = httpx.get(f"{API_BASE}/alerts/", params={"status": "pending", "limit": 20}, timeout=5.0)
        alerts = resp.json().get("alerts", []) if resp.status_code == 200 else []
    except Exception:
        alerts = []
        st.warning("Cannot reach ARCAS API. Start the API server with: make api")

    if not alerts:
        st.success("No pending alerts. Queue is empty.")
        return

    st.write(f"**{len(alerts)} alert(s) pending review**")

    for alert in alerts:
        alert_id  = alert.get("alert_id", "unknown")
        category  = alert.get("category", "?")
        score     = alert.get("confidence_score", 0.0)
        just      = alert.get("nl_justification", "No justification available")

        with st.expander(f"Alert {alert_id[:8]}... | Category {category} | Confidence {score:.1%}", expanded=False):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f"**Justification:** {just}")
                reasoning = alert.get("reasoning_chain", [])
                if reasoning:
                    st.markdown("**Reasoning Chain:**")
                    for step in reasoning:
                        st.markdown(f"- {step}")
            with col2:
                st.markdown("**Take Decision:**")
                operator_id    = st.text_input("Operator ID", value="analyst_01", key=f"op_{alert_id}")
                operator_notes = st.text_area("Notes", key=f"notes_{alert_id}", height=80)

                # 9 HITL action buttons
                from hitl.hitl_actions import submit_decision
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("✅ Approve",            key=f"approve_{alert_id}"):
                        submit_decision(alert_id, "approve",       operator_id, operator_notes)
                    if st.button("✏️ Modify & Approve",   key=f"modify_{alert_id}"):
                        submit_decision(alert_id, "modify",        operator_id, operator_notes)
                    if st.button("🔍 Request Evidence",   key=f"evidence_{alert_id}"):
                        submit_decision(alert_id, "evidence",      operator_id, operator_notes)
                    if st.button("📧 Send by Email",      key=f"email_{alert_id}"):
                        submit_decision(alert_id, "email",         operator_id, operator_notes)
                with c2:
                    if st.button("❌ Reject",             key=f"reject_{alert_id}"):
                        submit_decision(alert_id, "reject",        operator_id, operator_notes)
                    if st.button("⬆️ Escalate",           key=f"escalate_{alert_id}"):
                        submit_decision(alert_id, "escalate",      operator_id, operator_notes)
                    if st.button("👁️ Monitor",            key=f"monitor_{alert_id}"):
                        submit_decision(alert_id, "monitor",       operator_id, operator_notes)
                    if st.button("🚔 Report Authorities", key=f"report_{alert_id}"):
                        submit_decision(alert_id, "report",        operator_id, operator_notes)
                if st.button("🔴 False Positive",         key=f"fp_{alert_id}"):
                    submit_decision(alert_id, "false_positive", operator_id, operator_notes)
