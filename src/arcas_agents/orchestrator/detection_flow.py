"""
ARCAS - src/arcas_agents/orchestrator/detection_flow.py  (v2)

Improved scorer with:
- Judicial bias patterns (differential treatment by political affiliation)
- Temporal patterns (slow/fast proceedings)
- Enriched keywords for all 6 categories
"""
import json, logging, os, uuid
from typing import TypedDict, Annotated, Literal
import operator

from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

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
# Keyword sets per category (Spanish + common variants)
# ---------------------------------------------------------------------------

# Category A: Public procurement fraud
KW_CAT_A = [
    "contrato", "adjudicaci", "licitaci", "concurso", "subvencion",
    "contratacion publica", "obra publica", "empresa", "adjudicado",
    "pliego", "oferta", "presupuesto", "sobrecoste", "sobreprecio",
    "fraccionamiento", "comision", "mordida", "cohecho",
]

# Category B: Illicit enrichment / revolving doors
KW_CAT_B = [
    "patrimonio", "fortuna", "enriquecimiento", "bienes", "declaracion",
    "puerta giratoria", "fichaje", "exministro", "excargo", "exconcejal",
    "cuenta bancaria", "offshore", "paraiso fiscal", "sociedad pantalla",
    "testaferro", "filial", "holding",
]

# Category C: Judicial patterns - EXPANDED with bias detection
KW_CAT_C_GENERAL = [
    "juez", "tribunal", "sentencia", "fiscal", "imputado", "acusado",
    "diligencias", "auto", "sumario", "instruccion", "juzgado",
    "audiencia nacional", "tribunal supremo", "sala penal",
]

KW_CAT_C_BIAS = [
    # Slow/fast proceedings (differential treatment)
    "archivo", "archiva", "sobresee", "sobreseimiento", "prescripcion",
    "prescribe", "caducidad", "dilaciones", "retraso", "demora",
    "paralizado", "años sin juicio", "lustros", "decada",
    # Evidentiary basis anomalies
    "recorte de prensa", "recortes de periodico", "sin pruebas",
    "indicios", "sospechas", "fuente anonima", "confidente",
    "testigo protegido", "declaracion policial", "informe policial",
    "UCO", "UDEF",
    # Speed anomalies (suspiciously fast)
    "urgente", "rapidez", "celeridad", "en tiempo record",
    "inmediatamente detenido", "detenido en horas",
]

KW_CAT_C_POLITICAL = [
    # Political affiliation markers near judicial terms
    "psoe", "pp", "vox", "podemos", "sumar", "ciudadanos",
    "partido socialista", "partido popular", "militante", "dirigente",
    "cargo del", "miembro del", "afin al", "proximo al",
    "gobierno de", "oposicion",
]

# Category D: Disinformation
KW_CAT_D = [
    "bulo", "falso", "mentira", "desinformacion", "fake", "hoax",
    "manipulado", "desmentido", "verificado", "hecho falso",
    "fuera de contexto", "sin base", "no es cierto", "erroneo",
    "tuiteaba", "viralizado", "cadena de whatsapp",
]

# Category E: Influence networks
KW_CAT_E = [
    "red de contactos", "trama", "clan", "grupo organizado",
    "blanqueo", "financiacion ilegal", "donacion", "partido politico",
    "tesorero", "caja b", "fondos", "comisionista", "intermediario",
    "lobbista", "grupo presion",
]

# Category F: Abuse of public function
KW_CAT_F = [
    "nepotismo", "enchufismo", "amiguismo", "contratacion irregular",
    "cargo de confianza", "asesores", "liberados", "pluriempleo",
    "incompatibilidad", "conflicto de interes", "uso privado",
    "vehiculo oficial", "tarjeta de credito publica",
]


def _score_keywords(text: str, keywords: list[str]) -> float:
    """Score a text against a keyword list. Returns 0-1."""
    text_lower = text.lower()
    hits = sum(1 for kw in keywords if kw in text_lower)
    return min(hits * 0.15, 0.9)


def _detect_judicial_bias(text: str) -> float:
    """
    Detect potential judicial bias pattern:
    Co-occurrence of judicial terms + political affiliation markers
    + speed/evidentiary anomalies.
    High score = potential differential treatment pattern.
    """
    text_lower = text.lower()
    has_judicial   = any(kw in text_lower for kw in KW_CAT_C_GENERAL)
    has_political  = any(kw in text_lower for kw in KW_CAT_C_POLITICAL)
    has_bias       = any(kw in text_lower for kw in KW_CAT_C_BIAS)

    if has_judicial and has_political and has_bias:
        return 0.75   # Strong signal: all three present
    elif has_judicial and (has_political or has_bias):
        return 0.50   # Medium signal: two of three
    elif has_judicial:
        return 0.25   # Weak signal: judicial term only
    return 0.0


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
        "cat_a": _score_keywords(title, KW_CAT_A),
        "cat_b": _score_keywords(title, KW_CAT_B),
        "cat_c": _detect_judicial_bias(title),
        "cat_d": _score_keywords(title, KW_CAT_D),
        "cat_e": _score_keywords(title, KW_CAT_E),
        "cat_f": _score_keywords(title, KW_CAT_F),
    }

    # Boost for fact-checker sources (always worth analysing)
    if event.get("is_factchecker"):
        scores["cat_d"] = max(scores["cat_d"], 0.45)

    # Boost for gazette source on contracts
    if source == "gazette" and scores["cat_a"] > 0:
        scores["cat_a"] = min(scores["cat_a"] + 0.15, 0.9)

    overall = max(scores.values())
    log.info(f"Score {state['event_id'][:8]}: {overall:.2f} {scores}")
    return {
        "preliminary_score": overall,
        "score_breakdown":   scores,
        "reasoning_chain":   [f"Score: {overall:.2f} | {scores}"],
    }


def route_after_scoring(state: DetectionState) -> str:
    return "build_hypothesis" if state["preliminary_score"] >= SCORE_THRESHOLD \
           else "archive_below_threshold"


def build_hypothesis(state: DetectionState) -> dict:
    event     = state["event_data"]
    breakdown = state.get("score_breakdown", {})
    top_cat   = max(breakdown, key=breakdown.get) if breakdown else "cat_a"

    cat_descriptions = {
        "cat_a": "public procurement fraud or contract irregularity",
        "cat_b": "illicit enrichment, revolving doors or undeclared assets",
        "cat_c": "judicial pattern anomaly - potential differential treatment by political affiliation, evidentiary standard anomaly, or unexplained case speed disparity",
        "cat_d": "disinformation or false claim that may benefit identifiable actors",
        "cat_e": "influence network or illegal financing",
        "cat_f": "abuse of public function, nepotism or incompatibility",
    }

    if not GROQ_API_KEY or GROQ_API_KEY.startswith("gsk_YOUR"):
        hypothesis = f"[STUB] {cat_descriptions.get(top_cat,'unknown pattern')} detected in: {event.get('title','')[:100]}"
        return {"hypothesis": hypothesis, "reasoning_chain": [hypothesis], "status": "hypothesis_built"}

    llm    = ChatGroq(model=GROQ_MODEL, api_key=GROQ_API_KEY, max_tokens=400)
    prompt = (
        f"You are an anti-corruption analyst specialising in {cat_descriptions.get(top_cat,'corruption')}. "
        f"Analyse this public record and identify specific patterns. "
        f"If category C (judicial), specifically look for: differential speed of proceedings by political affiliation, "
        f"cases archived without examining evidence, UCO/UDEF reports delivered selectively, "
        f"prescription used to protect specific political actors. "
        f"Be factual. Cite only what is in the data. Never accuse - surface patterns only.\n\n"
        f"Record: {json.dumps(event, ensure_ascii=False)[:1500]}\n\n"
        f"Respond: 1) Category (A-F) 2) Specific pattern observed 3) Confidence 0-1 4) Who could benefit from this pattern"
    )
    response   = llm.invoke(prompt)
    hypothesis = response.content.strip()
    return {
        "hypothesis":    hypothesis,
        "reasoning_chain": [f"LLM ({top_cat}): {hypothesis[:300]}"],
        "status":        "hypothesis_built",
    }


def enrich_evidence(state: DetectionState) -> dict:
    return {
        "evidence_fragments": [state["event_data"]],
        "reasoning_chain":    ["Evidence: source fragment attached"],
        "status":             "evidence_enriched",
    }


def draft_alert(state: DetectionState) -> dict:
    alert_id  = str(uuid.uuid4())
    breakdown = state.get("score_breakdown", {})
    top_cat   = max(breakdown, key=breakdown.get) if breakdown else "cat_a"
    score     = min(state["preliminary_score"] + 0.10, 0.99)

    alert = {
        "alert_id":          alert_id,
        "category":          top_cat.replace("cat_", "").upper(),
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
        "alert_id":        alert_id,
        "alert_draft":     alert,
        "confidence_score": score,
        "reasoning_chain": [f"Alert {alert_id[:8]} cat={alert['category']} conf={score:.3f}"],
        "status":          "draft_ready",
    }


def hitl_review(state: DetectionState) -> dict:
    log.info(f"HITL: alert {state.get('alert_id','')[:8]} awaiting operator.")
    return {"status": "awaiting_hitl"}


def route_after_hitl(state: DetectionState) -> str:
    decision = state.get("hitl_decision", "")
    if decision in ("approve", "modify"):  return "publish_alert"
    if decision == "evidence":             return "enrich_evidence"
    return "archive_rejected"


def publish_alert(state: DetectionState) -> dict:
    import psycopg2
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
                json.dumps(alert["reasoning_chain"]),
                json.dumps({"source_name": alert.get("source_name",""),
                            "title": alert.get("title",""),
                            "content_url": alert.get("content_url","")}),
            ))
            conn.commit()
        conn.close()
        log.info(f"Alert {alert['alert_id'][:8]} published.")
    except Exception as e:
        log.error(f"Failed to persist alert: {e}")
    return {"status": "published"}


def archive_below_threshold(state: DetectionState) -> dict:
    return {"status": "archived_below_threshold"}


def archive_rejected(state: DetectionState) -> dict:
    return {"status": "archived_rejected"}


# ---------------------------------------------------------------------------
# Graph
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

    saver = MemorySaver()
    return graph.compile(checkpointer=saver, interrupt_before=["hitl_review"])


try:
    detection_graph = build_detection_graph()
    log.info("Detection graph compiled successfully.")
except Exception as e:
    log.error(f"Failed to compile detection graph: {e}")
    detection_graph = None
