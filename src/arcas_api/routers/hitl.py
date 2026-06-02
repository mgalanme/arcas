"""
ARCAS - src/arcas_api/routers/hitl.py
Human-in-the-loop decision endpoints.
Receives operator decisions and resumes the LangGraph detection flow.
"""
import logging
from typing import Literal
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os

router = APIRouter()
log    = logging.getLogger(__name__)


class HITLDecision(BaseModel):
    alert_id:      str
    decision:      Literal["approve", "reject", "modify", "evidence", "escalate",
                           "email", "report", "monitor", "false_positive"]
    operator_id:   str
    operator_notes: str = ""
    modified_alert: dict = {}


@router.post("/decision")
async def submit_decision(decision: HITLDecision):
    """
    Submit a HITL decision for an alert.
    For approve/reject/modify/evidence: resumes the LangGraph detection flow.
    For other actions: updates the alert status directly.
    """
    from src.arcas_agents.orchestrator.detection_flow import detection_graph

    FLOW_DECISIONS = {"approve", "reject", "modify", "evidence"}

    if decision.decision in FLOW_DECISIONS:
        # Resume the paused LangGraph flow
        config = {"configurable": {"thread_id": decision.alert_id}}
        try:
            detection_graph.update_state(
                config,
                {
                    "hitl_decision":  decision.decision,
                    "operator_notes": decision.operator_notes,
                },
                as_node="hitl_review",
            )
            # Invoke to resume from checkpoint
            detection_graph.invoke(None, config)
            return {"status": "resumed", "decision": decision.decision}
        except Exception as e:
            log.error(f"LangGraph resume failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    else:
        # Direct status update (email, report, monitor, false_positive, escalate)
        log.info(f"HITL action {decision.decision} for alert {decision.alert_id} by {decision.operator_id}")
        # Production: update PostgreSQL alert status + publish to Kafka audit topic
        return {"status": "actioned", "decision": decision.decision}


@router.get("/queue")
async def get_hitl_queue(limit: int = 20):
    """Get the pending HITL review queue."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PG_DSN = (
        f"host={os.getenv('POSTGRES_HOST','localhost')} "
        f"dbname={os.getenv('POSTGRES_DB','arcas')} "
        f"user={os.getenv('POSTGRES_USER','arcas_app')} "
        f"password={os.getenv('POSTGRES_PASSWORD','')}"
    )
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM alerts WHERE status='pending' ORDER BY confidence_score DESC LIMIT %s",
                (limit,)
            )
            return {"queue": cur.fetchall()}
    finally:
        conn.close()
