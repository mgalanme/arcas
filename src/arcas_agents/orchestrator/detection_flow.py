"""
ARCAS - src/arcas_agents/orchestrator/detection_flow.py

LangGraph main detection flow.
Stateful graph with PostgreSQL checkpointer.
Contains one mandatory HITL interrupt checkpoint before alert publication.

State transitions:
  START -> score_preliminary -> [archive | build_hypothesis]
        -> enrich_evidence -> draft_alert
        -> HITL_REVIEW (interrupt)
        -> [publish_alert | archive_rejected]
"""
import json, logging, os, uuid
from typing import TypedDict, Annotated, Literal
import operator
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

load_dotenv()
log = logging.getLogger(__name__)

GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL_PRIMARY", "llama-3.3-70b-versatile")
PG_CONN_STRING  = (
    f"postgresql://{os.getenv('POSTGRES_USER','arcas_app')}:"
    f"{os.getenv('POSTGRES_PASSWORD','')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5432')}/"
    f"{os.getenv('POSTGRES_DB','arcas')}"
)
SCORE_THRESHOLD = 0.35   # Minimum score to proceed to hypothesis building


# ---------------------------------------------------------------------------
# State definition
# ---------------------------------------------------------------------------

class DetectionState(TypedDict):
    # Input
    event_id:           str
    event_data:         dict
    # Scoring
    preliminary_score:  float
    score_breakdown:    dict
    # Hypothesis
    hypothesis:         str
    supporting_events:  Annotated[list, operator.add]
    evidence_fragments: Annotated[list, operator.add]
    # Alert
    alert_draft:        dict
    confidence_score:   float
    reasoning_chain:    Annotated[list, operator.add]
    # HITL
    hitl_decision:      Literal["approve", "reject", "modify", "evidence"] | None
    operator_notes:     str
    # Control
    status:             str
    alert_id:           str


# ---------------------------------------------------------------------------
# Node functions
# ---------------------------------------------------------------------------

def score_preliminary(state: DetectionState) -> dict:
    """Fast heuristic scoring across all 6 categories."""
    event  = state["event_data"]
    scores = {}

    # Category A: procurement fraud signals
    title = (event.get("title") or "").lower()
    scores["cat_a"] = 0.0
    if any(kw in title for kw in ["contrato", "adjudicacion", "licitacion", "concurso"]):
        scores["cat_a"] = 0.4
    if event.get("source_type") == "procurement":
        scores["cat_a"] += 0.2

    # Category D: disinformation signals
    scores["cat_d"] = 0.0
    if event.get("source_type") == "media":
        scores["cat_d"] = 0.1

    # Overall: max of individual scores (simplified heuristic)
    overall = max(scores.values()) if scores else 0.0

    log.info(f"Preliminary score for {state['event_id']}: {overall:.3f}")
    return {
        "preliminary_score": overall,
        "score_breakdown": scores,
        "reasoning_chain": [f"Preliminary heuristic score: {overall:.3f}. Breakdown: {scores}"],
    }


def route_after_scoring(state: DetectionState) -> str:
    if state["preliminary_score"] >= SCORE_THRESHOLD:
        return "build_hypothesis"
    return "archive_below_threshold"


def build_hypothesis(state: DetectionState) -> dict:
    """Use LLM to build a structured hypothesis from event data."""
    llm = ChatGroq(model=GROQ_MODEL, api_key=GROQ_API_KEY, max_tokens=500)
    event = state["event_data"]
    prompt = f"""You are an anti-corruption analyst. Analyse this public record and identify 
any potential corruption or fraud patterns. Be concise and factual. Cite only what is in the data.

Record: {json.dumps(event, ensure_ascii=False)[:2000]}

Respond with: 1) Pattern category (A-F) 2) Hypothesis in one sentence 3) Confidence (0-1)"""

    response = llm.invoke(prompt)
    hypothesis = response.content.strip()

    return {
        "hypothesis": hypothesis,
        "reasoning_chain": [f"LLM hypothesis: {hypothesis[:200]}"],
        "status": "hypothesis_built",
    }


def enrich_evidence(state: DetectionState) -> dict:
    """Search for additional supporting evidence (simplified stub)."""
    # In production: query Qdrant for semantically similar evidence,
    # query Neo4j for related actors, call enrichment agent.
    return {
        "evidence_fragments": [state["event_data"]],
        "reasoning_chain": ["Evidence enrichment: 1 fragment from source event"],
        "status": "evidence_enriched",
    }


def draft_alert(state: DetectionState) -> dict:
    """Generate the alert draft with full justification."""
    alert_id = str(uuid.uuid4())
    score    = min(state["preliminary_score"] + 0.2, 1.0)

    alert = {
        "alert_id":          alert_id,
        "category":          "A",   # Simplified; real flow uses LLM classification
        "confidence_score":  score,
        "nl_justification":  state["hypothesis"],
        "reasoning_chain":   state["reasoning_chain"],
        "supporting_events": state["supporting_events"] or [state["event_id"]],
        "status":            "pending",
    }

    return {
        "alert_id":       alert_id,
        "alert_draft":    alert,
        "confidence_score": score,
        "reasoning_chain": [f"Draft alert created: {alert_id}, confidence={score:.3f}"],
        "status":         "draft_ready",
    }


def hitl_review(state: DetectionState) -> dict:
    """
    HITL interrupt checkpoint.
    This node pauses the graph and waits for human decision.
    The graph resumes when an operator calls graph.update_state()
    with hitl_decision = 'approve' | 'reject' | 'modify' | 'evidence'
    """
    log.info(f"HITL checkpoint reached for alert {state['alert_id']}. Waiting for operator.")
    return {"status": "awaiting_hitl"}


def route_after_hitl(state: DetectionState) -> str:
    decision = state.get("hitl_decision")
    if decision == "approve":   return "publish_alert"
    if decision == "modify":    return "publish_alert"
    if decision == "evidence":  return "enrich_evidence"
    return "archive_rejected"


def publish_alert(state: DetectionState) -> dict:
    """Persist the validated alert to PostgreSQL and update actor risk scores."""
    log.info(f"Publishing alert {state['alert_id']} (decision: {state['hitl_decision']})")
    # In production: INSERT into alerts table, update actor risk scores in Neo4j,
    # publish to arcas.alerts.validated Kafka topic.
    return {"status": "published"}


def archive_below_threshold(state: DetectionState) -> dict:
    log.info(f"Event {state['event_id']} archived (score below threshold)")
    return {"status": "archived_below_threshold"}


def archive_rejected(state: DetectionState) -> dict:
    log.info(f"Alert {state['alert_id']} rejected by operator")
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
    graph.add_conditional_edges("score_preliminary", route_after_scoring,
        {"build_hypothesis": "build_hypothesis",
         "archive_below_threshold": "archive_below_threshold"})
    graph.add_edge("build_hypothesis", "enrich_evidence")
    graph.add_edge("enrich_evidence",  "draft_alert")
    graph.add_edge("draft_alert",      "hitl_review")
    graph.add_conditional_edges("hitl_review", route_after_hitl,
        {"publish_alert":    "publish_alert",
         "archive_rejected": "archive_rejected",
         "enrich_evidence":  "enrich_evidence"})
    graph.add_edge("publish_alert",           END)
    graph.add_edge("archive_below_threshold", END)
    graph.add_edge("archive_rejected",        END)

    # PostgreSQL checkpointer for stateful resumption after HITL
    checkpointer = PostgresSaver.from_conn_string(PG_CONN_STRING)
    checkpointer.setup()

    return graph.compile(
        checkpointer=checkpointer,
        interrupt_before=["hitl_review"],   # PAUSE before HITL node
    )


# Module-level compiled graph (import this in other modules)
detection_graph = build_detection_graph()
