"""
ARCAS - src/arcas_dashboard/hitl/hitl_actions.py

HITL action handlers for the Streamlit dashboard.
Each action is sent to the ARCAS API, which handles the business logic
and audit logging. The dashboard only triggers and displays results.
"""
import httpx
import streamlit as st

API_BASE = "http://localhost:8000/api/v1"

ACTION_LABELS = {
    "approve":       ("✅ Approved",          "success"),
    "reject":        ("❌ Rejected",          "error"),
    "modify":        ("✏️ Modified & Approved", "success"),
    "evidence":      ("🔍 Evidence Requested", "info"),
    "escalate":      ("⬆️ Escalated",         "warning"),
    "email":         ("📧 Sent by Email",      "info"),
    "report":        ("🚔 Reported to Authorities", "warning"),
    "monitor":       ("👁️ Archived for Monitoring", "info"),
    "false_positive":("🔴 Marked False Positive", "error"),
}


def submit_decision(alert_id: str, decision: str, operator_id: str, notes: str = "") -> None:
    """Submit a HITL decision to the API and show feedback in Streamlit."""

    # Special confirmation for irreversible actions
    if decision == "report":
        st.warning(
            "⚠️ REPORT TO AUTHORITIES: This generates a formal complaint document. "
            "You must manually submit it through the appropriate official channel. "
            "This action is logged permanently."
        )

    try:
        resp = httpx.post(
            f"{API_BASE}/hitl/decision",
            json={
                "alert_id":      alert_id,
                "decision":      decision,
                "operator_id":   operator_id,
                "operator_notes": notes,
            },
            timeout=10.0,
        )
        if resp.status_code == 200:
            label, msg_type = ACTION_LABELS.get(decision, (decision, "info"))
            if msg_type == "success":
                st.success(f"{label} — alert {alert_id[:8]}")
            elif msg_type == "error":
                st.error(f"{label} — alert {alert_id[:8]}")
            elif msg_type == "warning":
                st.warning(f"{label} — alert {alert_id[:8]}")
            else:
                st.info(f"{label} — alert {alert_id[:8]}")
        else:
            st.error(f"API error {resp.status_code}: {resp.text[:200]}")
    except httpx.ConnectError:
        st.error("Cannot reach ARCAS API. Is the API server running? (make api)")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
