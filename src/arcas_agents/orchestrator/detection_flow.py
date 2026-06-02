"""
ARCAS - src/arcas_agents/orchestrator/detection_flow.py

LangGraph detection flow - adapted for LangGraph 1.1.x
Stateful graph with PostgreSQL checkpointer.
One mandatory HITL interrupt before alert publication.

Flow:
  score_preliminary -> [archive | build_hypothesis]
  build_hypothesis -> enrich_evidence -> draft_alert
  -> HITL interrupt (awaiting operator decision)
  -> [publish_alert | archive_rejected | enrich_evidence]
"""
import json, logging, os, uuid
from typing import TypedDict, Annotated, Literal
import operator

from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver
import psycopg

load_dotenv()
log = logging.getLogger(__name__)

GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL_PRIMARY", "llama-3.3-70b-versatile")
SCORE_THRESHOLD = 0.30

PG_CONN = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
class DetectionState(TypedDict):
    event_id:           str
    event_data:         dict
    preliminary_score:  float
    score_breakdown:    dict
    hypothesis:         str
    supporting_events:  Annotated[list, operator.add]
    evidence_fragments: Annotated[list, operator.add]
    alert_draft:        dict
    confidence_score:   float
    reasoning_chain:    Annotated[list, operator.add]
    hitl_decision:      str | None
    operator_notes:     str
    status:             str
    alert_id:           str


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------
def score_preliminary(state: DetectionState) -> dict:
    event  = state["event_data"]
    title  = (event.get("title") or "").lower()
    source = event.get("source_type", "")
    scores = {
        "cat_a": 0.0, "cat_b": 0.0, "cat_c": 0.0,
        "cat_d": 0.0, "cat_e": 0.0, "cat_f": 0.0,
    }

    # Category A: procurement signals in gazette
    if any(k in title for k in ["contrato", "adjudicaci", "licitaci", "concurso"]):
        scores["cat_a"] = 0.5
    if source == "gazette":
        scores["cat_a"] += 0.1

    # Category C: judicial signals
    if any(k in title for k in ["juez", "tribunal", "sentencia", "fiscal", "imputado"]):
        scores["cat_c"] = 0.5

    # Category D: disinformation signals from fact-checkers
    if event.get("is_factchecker"):
        scores["cat_d"] = 0.4

    # Category D: media with bulo-related keywords
    if any(k in title for k in ["bulo", "falso", "desinformaci", "mentira", "fake"]):
        scores["cat_d"] = max(scores["cat_d"], 0.5)

    overall = max(scores.values())
    log.info(f"Score {state['event_id'][:8]}: {overall:.2f} {scores}")
    return {
        "preliminary_score": overall,
        "score_breakdown":   scores,
        "reasoning_chain": [f"Preliminary score: {overall:.2f} | breakdown: {scores}"],
    }


def route_after_scoring(state: DetectionState) -> str:
    return "build_hypothesis" if state["preliminary_score"] >= SCORE_THRESHOLD \
           else "archive_below_threshold"


def build_hypothesis(state: DetectionState) -> dict:
    if not GROQ_API_KEY or GROQ_API_KEY.startswith("gsk_YOUR"):
        hypothesis = f"[STUB] Pattern detected in: {state['event_data'].get('title','')[:100]}"
        return {
            "hypothesis":    hypothesis,
            "reasoning_chain": [f"Hypothesis (stub): {hypothesis}"],
            "status":        "hypothesis_built",
        }

    llm    = ChatGroq(model=GROQ_MODEL, api_key=GROQ_API_KEY, max_tokens=300)
    event  = state["event_data"]
    prompt = (
        "You are an anti-corruption analyst. Analyse this public record and identify "
        "any potential corruption, fraud or disinformation patterns. Be concise and factual. "
        "Cite only what is in the data. Respond with: "
        "1) Pattern category (A=procurement fraud, B=enrichment, C=judicial, D=disinformation, E=networks, F=abuse) "
        "2) One-sentence hypothesis 3) Confidence 0-1\n\n"
        f"Record: {json.dumps(event, ensure_ascii=False)[:1500]}"
    )
    response   = llm.invoke(prompt)
    hypothesis = response.content.strip()
    return {
        "hypothesis":    hypothesis,
        "reasoning_chain": [f"LLM hypothesis: {hypothesis[:200]}"],
        "status":        "hypothesis_built",
    }


def enrich_evidence(state: DetectionState) -> dict:
    return {
        "evidence_fragments": [state["event_data"]],
        "reasoning_chain":    ["Evidence: 1 source fragment"],
        "status":             "evidence_enriched",
    }


def draft_alert(state: DetectionState) -> dict:
    alert_id = str(uuid.uuid4())
    score    = min(state["preliminary_score"] + 0.15, 0.99)

    # Determine category from score breakdown
    breakdown = state.get("score_breakdown", {})
    category  = max(breakdown, key=breakdown.get) if breakdown else "cat_a"
    cat_label = category.replace("cat_", "").upper()

    alert = {
        "alert_id":          alert_id,
        "category":          cat_label,
        "confidence_score":  round(score, 3),
        "nl_justification":  state["hypothesis"],
        "reasoning_chain":   state["reasoning_chain"],
        "supporting_events": [state["event_id"]],
        "source_name":       state["event_data"].get("source_name", ""),
        "title":             state["event_data"].get("title", ""),
        "content_url":       state["event_data"].get("content_url", ""),
        "status":            "pending",
    }
    return {
        "alert_id":       alert_id,
        "alert_draft":    alert,
        "confidence_score": score,
        "reasoning_chain": [f"Draft alert {alert_id[:8]} cat={cat_label} conf={score:.3f}"],
        "status":         "draft_ready",
    }


def hitl_review(state: DetectionState) -> dict:
    """
    HITL interrupt node. Graph pauses here.
    Resumes when operator calls graph.update_state() with hitl_decision.
    """
    log.info(f"HITL checkpoint: alert {state.get('alert_id','')[:8]} awaiting operator.")
    return {"status": "awaiting_hitl"}


def route_after_hitl(state: DetectionState) -> str:
    decision = state.get("hitl_decision", "")
    if decision in ("approve", "modify"):  return "publish_alert"
    if decision == "evidence":             return "enrich_evidence"
    return "archive_rejected"


def publish_alert(state: DetectionState) -> dict:
    """Persist validated alert to PostgreSQL."""
    import psycopg2, json as _json
    alert = state["alert_draft"]
    try:
        conn = psycopg2.connect(PG_CONN)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts
                    (alert_id, category, status, confidence_score,
                     nl_justification, reasoning_chain, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (alert_id) DO UPDATE SET status = EXCLUDED.status
            """, (
                alert["alert_id"],
                alert["category"],
                "approved" if state.get("hitl_decision") == "approve" else "modified_approved",
                alert["confidence_score"],
                alert["nl_justification"],
                _json.dumps(alert["reasoning_chain"]),
                _json.dumps({
                    "source_name": alert.get("source_name",""),
                    "title":       alert.get("title",""),
                    "content_url": alert.get("content_url",""),
                }),
            ))
            conn.commit()
        conn.close()
        log.info(f"Alert {alert['alert_id'][:8]} published to PostgreSQL.")
    except Exception as e:
        log.error(f"Failed to persist alert: {e}")
    return {"status": "published"}


def archive_below_threshold(state: DetectionState) -> dict:
    log.info(f"Event {state['event_id'][:8]} archived (score below threshold)")
    return {"status": "archived_below_threshold"}


def archive_rejected(state: DetectionState) -> dict:
    log.info(f"Alert {state.get('alert_id','?')[:8]} rejected by operator")
    return {"status": "archived_rejected"}


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------
def build_detection_graph():
    graph = StateGraph(DetectionState)
    graph.add_node("score_preliminary",       score_preliminary)
    graph.add_node("build_hypothesis",        build_hypothesis)
    graph.add_node("enrich_evidence",         enrich_evidence)
    graph.add_node("draft_alert",             draft_alert)
    graph.add_node("hitl_review",             hitl_review)
    graph.add_node("publish_alert",           publish_alert)
    graph.add_node("archive_below_threshold", archive_below_threshold)
    graph.add_node("archive_rejected",        archive_rejected)

    graph.set_entry_point("score_preliminary")
    graph.add_conditional_edges("score_preliminary", route_after_scoring, {
        "build_hypothesis":        "build_hypothesis",
        "archive_below_threshold": "archive_below_threshold",
    })
    graph.add_edge("build_hypothesis", "enrich_evidence")
    graph.add_edge("enrich_evidence",  "draft_alert")
    graph.add_edge("draft_alert",      "hitl_review")
    graph.add_conditional_edges("hitl_review", route_after_hitl, {
        "publish_alert":    "publish_alert",
        "archive_rejected": "archive_rejected",
        "enrich_evidence":  "enrich_evidence",
    })
    graph.add_edge("publish_alert",           END)
    graph.add_edge("archive_below_threshold", END)
    graph.add_edge("archive_rejected",        END)

    # Use in-memory checkpointer for Phase 1 demo
    # PostgreSQL checkpointer will replace this in Phase 2
    # when LangGraph and langgraph-checkpoint-postgres versions are aligned
    from langgraph.checkpoint.memory import MemorySaver
    saver = MemorySaver()

    return graph.compile(
        checkpointer=saver,
        interrupt_before=["hitl_review"],
    )


# Module-level graph - import this in run_agents.py
try:
    detection_graph = build_detection_graph()
    log.info("Detection graph compiled successfully.")
except Exception as e:
    log.error(f"Failed to compile detection graph: {e}")
    detection_graph = None
