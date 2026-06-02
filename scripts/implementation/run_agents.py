"""
ARCAS - scripts/implementation/run_agents.py

Runs the detection agent flow over records waiting in arcas.processed.
Consumes from the processed topic and invokes the LangGraph detection flow
for each record above the preliminary scoring threshold.

Usage:
  PYTHONPATH=. python scripts/implementation/run_agents.py
  PYTHONPATH=. python scripts/implementation/run_agents.py --limit 10
"""
import argparse, json, logging, os, time, uuid
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_IN        = "arcas.processed"
CONSUMER_GROUP  = "arcas-agent-runner"


def create_consumer():
    for i in range(10):
        try:
            return KafkaConsumer(
                TOPIC_IN,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode()),
                consumer_timeout_ms=10000,
            )
        except NoBrokersAvailable:
            time.sleep(3)
    raise RuntimeError("Cannot connect to Kafka")


def run_detection(event_data: dict) -> None:
    """Run the LangGraph detection flow for a single event."""
    from src.arcas_agents.orchestrator.detection_flow import detection_graph

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
        log.info(f"Event {event_id[:8]}: status={status}")

        if status == "awaiting_hitl":
            log.info(f"  -> Alert queued for HITL review (alert_id={result.get('alert_id','')[:8]})")
    except Exception as e:
        log.error(f"Detection flow error for {event_id[:8]}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Max records to process (0=unlimited)")
    args = parser.parse_args()

    log.info(f"Agent runner starting (limit={args.limit or 'unlimited'})")
    consumer  = create_consumer()
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
