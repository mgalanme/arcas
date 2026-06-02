"""
ARCAS - scripts/implementation/run_agents.py  (fix C-08)

C-08 fix: thread_id is now alert_id (generated in draft_alert node).
The thread_id used by LangGraph and the alert_id stored in PostgreSQL
are the same identifier, so the HITL endpoint can correctly resume
the paused graph using the alert_id it receives from the operator.
"""
import argparse, json, logging, os, time, uuid
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
PG_CONN = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)


def create_consumer(topic: str):
    for i in range(10):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                group_id="arcas-agent-runner",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode()),
                consumer_timeout_ms=10000,
            )
        except NoBrokersAvailable:
            time.sleep(3)
    raise RuntimeError("Cannot connect to Kafka")


def persist_pending_alert(alert: dict, thread_id: str) -> None:
    """
    Persist a draft alert to PostgreSQL with status=pending.
    Stores thread_id in metadata so the HITL endpoint can resume the graph.
    C-08: thread_id == alert_id, so the HITL endpoint always knows
    which LangGraph thread to resume.
    """
    import psycopg2
    try:
        conn = psycopg2.connect(PG_CONN)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts
                    (alert_id, category, status, confidence_score,
                     nl_justification, reasoning_chain, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (alert_id) DO NOTHING
            """, (
                alert["alert_id"],
                alert["category"],
                "pending",
                alert["confidence_score"],
                alert["nl_justification"],
                json.dumps(alert.get("reasoning_chain", [])),
                json.dumps({
                    "source_name":  alert.get("source_name", ""),
                    "title":        alert.get("title", ""),
                    "content_url":  alert.get("content_url", ""),
                    "thread_id":    thread_id,   # C-08: stored for HITL resume
                }),
            ))
            conn.commit()
        conn.close()
        log.info(f"  Alert {alert['alert_id'][:8]} persisted to PostgreSQL (thread_id={thread_id[:8]})")
    except Exception as e:
        log.error(f"  Failed to persist alert: {e}")


def run_detection(event_data: dict) -> str:
    """
    Run the LangGraph detection flow.
    C-08 fix: thread_id is pre-generated as a UUID and will become
    the alert_id when draft_alert node runs. This ensures LangGraph
    checkpoint key == PostgreSQL alert_id == HITL resume key.
    """
    from src.arcas_agents.orchestrator.detection_flow import detection_graph

    if detection_graph is None:
        log.error("Detection graph failed to compile.")
        return "error"

    # C-08: pre-generate alert_id and use it as thread_id
    # The detection_flow will use this same UUID as alert_id in draft_alert
    thread_id = str(uuid.uuid4())
    config    = {"configurable": {"thread_id": thread_id}}

    initial_state = {
        "event_id":           event_data.get("content_hash", str(uuid.uuid4())),
        "event_data":         event_data,
        "preliminary_score":  0.0,
        "score_breakdown":    {},
        "hypothesis":         "",
        "supporting_events":  [],
        "evidence_fragments": [],
        "alert_draft":        {},
        "confidence_score":   0.0,
        "reasoning_chain":    [],
        "hitl_decision":      None,
        "operator_notes":     "",
        "status":             "new",
        "alert_id":           thread_id,   # C-08: pass thread_id as alert_id
    }

    try:
        result = detection_graph.invoke(initial_state, config)
        status = result.get("status", "unknown")
        title  = event_data.get("title", "")[:60]
        log.info(f"Event {initial_state['event_id'][:8]}: status={status} | '{title}'")

        if status == "awaiting_hitl":
            alert = result.get("alert_draft", {})
            log.info(
                f"  -> HITL alert: cat={alert.get('category','?')} "
                f"conf={alert.get('confidence_score',0):.2f} "
                f"alert_id={thread_id[:8]}"
            )
            # Persist to PostgreSQL so the dashboard can show it
            if alert:
                persist_pending_alert(alert, thread_id)

        return status
    except Exception as e:
        log.error(f"Detection flow error: {e}")
        return "error"


SYNTHETIC_EVENTS = [
    {
        "source_type": "gazette", "source_name": "BOE",
        "title": "Adjudicación directa a empresa vinculada a cargo público por 499.000 euros",
        "content_url": "https://example.test/s1",
        "content_hash": str(uuid.uuid4()),
        "language": "es", "nlp_processed": True,
        "entities": [], "entity_proposals": [],
        "embedding_vector": [0.0] * 768,
        "is_synthetic": True,
    },
    {
        "source_type": "media", "source_name": "El País",
        "title": "El juez archiva la causa sin examinar las pruebas aportadas por la acusación",
        "content_url": "https://example.test/s2",
        "content_hash": str(uuid.uuid4()),
        "language": "es", "nlp_processed": True,
        "entities": [], "entity_proposals": [],
        "embedding_vector": [0.0] * 768,
        "is_synthetic": True,
    },
    {
        "source_type": "factcheck", "source_name": "Maldita.es",
        "title": "Es falso que el gobierno haya subido los impuestos a las clases medias",
        "content_url": "https://example.test/s3",
        "content_hash": str(uuid.uuid4()),
        "language": "es", "nlp_processed": True,
        "is_factchecker": True,
        "entities": [], "entity_proposals": [],
        "embedding_vector": [0.0] * 768,
        "is_synthetic": True,
    },
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARCAS agent runner")
    parser.add_argument("--limit",     type=int, default=0)
    parser.add_argument("--topic",     default="arcas.nlp.extracted")
    parser.add_argument("--synthetic", action="store_true")
    args = parser.parse_args()

    log.info(f"Agent runner starting (limit={args.limit or 'unlimited'}, "
             f"topic={args.topic}, synthetic={args.synthetic})")

    if args.synthetic:
        for event in SYNTHETIC_EVENTS:
            run_detection(event)
        log.info("Synthetic run complete.")
    else:
        consumer  = create_consumer(args.topic)
        processed = 0
        try:
            for message in consumer:
                run_detection(message.value)
                processed += 1
                if args.limit and processed >= args.limit:
                    break
        finally:
            consumer.close()
            log.info(f"Agent runner stopped. Processed: {processed}")
