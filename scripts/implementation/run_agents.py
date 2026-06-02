"""
ARCAS - scripts/implementation/run_agents.py

Runs the LangGraph detection flow over NLP-processed records.

In production: reads from arcas.processed (after pseudonymisation vault).
In Phase 1 (current): reads from arcas.nlp.extracted directly.

Usage:
  PYTHONPATH=. python scripts/implementation/run_agents.py --limit 5
  PYTHONPATH=. python scripts/implementation/run_agents.py --limit 5 --topic arcas.nlp.extracted
  PYTHONPATH=. python scripts/implementation/run_agents.py --synthetic
"""
import argparse, json, logging, os, time, uuid
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")


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


def run_detection(event_data: dict, dry_run: bool = False) -> str:
    """Run the LangGraph detection flow for a single event."""
    from src.arcas_agents.orchestrator.detection_flow import detection_graph

    if detection_graph is None:
        log.error("Detection graph failed to compile. Check logs above.")
        return "error"

    event_id  = event_data.get("content_hash", str(uuid.uuid4()))
    thread_id = f"event-{event_id[:16]}"
    config    = {"configurable": {"thread_id": thread_id}}

    initial_state = {
        "event_id":           event_id,
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
        "alert_id":           "",
    }

    try:
        result = detection_graph.invoke(initial_state, config)
        status = result.get("status", "unknown")
        title  = event_data.get("title", "")[:60]
        log.info(f"Event {event_id[:8]}: status={status} | '{title}'")
        if status == "awaiting_hitl":
            alert = result.get("alert_draft", {})
            log.info(f"  -> HITL alert: cat={alert.get('category','?')} "
                     f"conf={alert.get('confidence_score',0):.2f} "
                     f"alert_id={alert.get('alert_id','?')[:8]}")
        return status
    except Exception as e:
        log.error(f"Detection flow error for {event_id[:8]}: {e}")
        return "error"


SYNTHETIC_EVENTS = [
    {
        "source_type": "gazette", "source_name": "BOE",
        "title": "Adjudicación directa de contrato a empresa vinculada a cargo público por importe de 499.000 euros",
        "content_url": "https://example.test/synthetic",
        "content_hash": str(uuid.uuid4()),
        "language": "es", "nlp_processed": True,
        "entities": [], "entity_proposals": [],
        "embedding_vector": [0.0] * 768,
        "is_synthetic": True,
    },
    {
        "source_type": "media", "source_name": "El País",
        "title": "El juez archiva la causa contra el político sin examinar las pruebas aportadas por la acusación",
        "content_url": "https://example.test/synthetic2",
        "content_hash": str(uuid.uuid4()),
        "language": "es", "nlp_processed": True,
        "entities": [], "entity_proposals": [],
        "embedding_vector": [0.0] * 768,
        "is_synthetic": True,
    },
    {
        "source_type": "factcheck", "source_name": "Maldita.es",
        "title": "Es falso que el gobierno haya subido los impuestos a las clases medias",
        "content_url": "https://example.test/synthetic3",
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
    parser.add_argument("--limit",     type=int, default=0,
                        help="Max records to process (0=unlimited)")
    parser.add_argument("--topic",     default="arcas.nlp.extracted",
                        help="Kafka input topic (default: arcas.nlp.extracted)")
    parser.add_argument("--synthetic", action="store_true",
                        help="Use synthetic test events instead of Kafka")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Score only, do not call LLM")
    args = parser.parse_args()

    log.info(f"Agent runner starting (limit={args.limit or 'unlimited'}, "
             f"topic={args.topic}, synthetic={args.synthetic})")

    if args.synthetic:
        events = SYNTHETIC_EVENTS
        log.info(f"Using {len(events)} synthetic events")
        for event in events:
            run_detection(event, dry_run=args.dry_run)
        log.info("Synthetic run complete.")
    else:
        consumer  = create_consumer(args.topic)
        processed = 0
        try:
            for message in consumer:
                run_detection(message.value, dry_run=args.dry_run)
                processed += 1
                if args.limit and processed >= args.limit:
                    break
        finally:
            consumer.close()
            log.info(f"Agent runner stopped. Processed: {processed}")
