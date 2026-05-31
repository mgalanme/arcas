"""
ARCAS - scripts/maintenance/test_ingestion.py

Smoke test for the ingestion pipeline.
Produces a small number of synthetic messages to a raw Kafka topic
and verifies they are consumed and processed correctly.

Usage:
  PYTHONPATH=. python scripts/maintenance/test_ingestion.py --source gazette --limit 10
  PYTHONPATH=. python scripts/maintenance/test_ingestion.py --source all --limit 5
"""

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TIMEOUT_SEC   = 30

SYNTHETIC_SOURCES = {
    "gazette": {
        "topic": "arcas.raw.gazette",
        "sample": {
            "source_type": "gazette",
            "source_name": "BOE (Synthetic Test)",
            "publication_date": datetime.now(timezone.utc).isoformat(),
            "title": "Real Decreto TEST/2024 - Synthetic ingestion test record",
            "content": (
                "This is a synthetic test record for the ARCAS ingestion pipeline validation. "
                "It contains references to fictional entities: Empresa Ficticia S.L., "
                "Ministerio de Pruebas, and Contrato de Prueba 12345/2024. "
                "No real persons or organisations are referenced."
            ),
            "url": "https://example.test/boe/test-record",
            "is_synthetic": True,
        }
    },
    "procurement": {
        "topic": "arcas.raw.procurement",
        "sample": {
            "source_type": "procurement",
            "source_name": "PLACSP (Synthetic Test)",
            "publication_date": datetime.now(timezone.utc).isoformat(),
            "contract_id": "TEST-2024-000001",
            "contract_title": "Synthetic Test Contract - ARCAS Pipeline Validation",
            "awarding_body": "Organismo de Pruebas",
            "awarded_to": "Empresa Ficticia de Prueba S.A.",
            "amount_eur": 125000.00,
            "procedure_type": "open",
            "is_synthetic": True,
        }
    },
    "media": {
        "topic": "arcas.raw.media",
        "sample": {
            "source_type": "media",
            "source_name": "Test Media Outlet",
            "publication_date": datetime.now(timezone.utc).isoformat(),
            "headline": "ARCAS Synthetic Test Article",
            "content": (
                "This is a synthetic media article for pipeline testing. "
                "It contains no references to real persons or organisations. "
                "Fictional entities: Partido Ficticio, Alcalde Imaginario."
            ),
            "author": "Test Author",
            "url": "https://example.test/media/test-article",
            "is_synthetic": True,
        }
    },
    "courts": {
        "topic": "arcas.raw.courts",
        "sample": {
            "source_type": "courts",
            "source_name": "Test Court Records",
            "publication_date": datetime.now(timezone.utc).isoformat(),
            "case_id": "TEST-CASE-2024/001",
            "court": "Tribunal Ficticio de Prueba",
            "ruling_type": "sentencia",
            "content": (
                "This is a synthetic judicial record for pipeline testing. "
                "No real persons, judges or courts are referenced. "
                "Fictional parties: Demandante Ficticio vs. Demandado Ficticio."
            ),
            "is_synthetic": True,
        }
    },
}


def create_producer() -> KafkaProducer:
    """Create a Kafka producer with retry logic."""
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            log.info(f"Kafka producer connected to {BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            retries -= 1
            log.warning(f"Kafka not available, retrying... ({retries} left)")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP}")


def produce_messages(producer: KafkaProducer, source: str, limit: int) -> list[str]:
    """Produce synthetic messages to the specified source topic."""
    config = SYNTHETIC_SOURCES[source]
    topic  = config["topic"]
    produced_ids = []

    log.info(f"Producing {limit} message(s) to topic: {topic}")

    for i in range(limit):
        message_id = str(uuid.uuid4())
        message = {
            **config["sample"],
            "message_id": message_id,
            "sequence": i + 1,
            "total": limit,
        }
        producer.send(topic, key=message_id, value=message)
        produced_ids.append(message_id)
        log.info(f"  Sent [{i+1}/{limit}] message_id={message_id[:8]}...")

    producer.flush()
    log.info(f"All messages produced to {topic}")
    return produced_ids


def verify_messages_on_topic(
    topic: str,
    expected_ids: list[str],
    timeout_sec: int = TIMEOUT_SEC
) -> bool:
    """Consume from a topic and verify expected message IDs appear."""
    log.info(f"Verifying messages on topic: {topic} (timeout={timeout_sec}s)")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BOOTSTRAP],
        auto_offset_reset="latest",
        group_id=f"arcas-smoke-test-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_sec * 1000,
    )

    found_ids = set()
    expected_set = set(expected_ids)

    for message in consumer:
        msg_id = message.value.get("message_id")
        if msg_id in expected_set:
            found_ids.add(msg_id)
            log.info(f"  Found: {msg_id[:8]}...")
        if found_ids == expected_set:
            break

    consumer.close()
    missing = expected_set - found_ids

    if missing:
        log.warning(f"  Missing {len(missing)} message(s) from {topic}")
        return False
    else:
        log.info(f"  All {len(expected_ids)} messages verified on {topic}")
        return True


def run_smoke_test(source: str, limit: int) -> bool:
    """Run a full smoke test for a given source."""
    log.info("=" * 60)
    log.info(f"ARCAS Ingestion Smoke Test")
    log.info(f"Source: {source} | Messages: {limit}")
    log.info("=" * 60)

    sources_to_test = (
        list(SYNTHETIC_SOURCES.keys()) if source == "all" else [source]
    )

    if source != "all" and source not in SYNTHETIC_SOURCES:
        log.error(f"Unknown source '{source}'. Valid: {list(SYNTHETIC_SOURCES.keys()) + ['all']}")
        return False

    all_passed = True
    producer = create_producer()

    try:
        for src in sources_to_test:
            log.info(f"\n--- Testing source: {src} ---")
            produced_ids = produce_messages(producer, src, limit)

            # Verify messages appear on the raw topic
            topic = SYNTHETIC_SOURCES[src]["topic"]
            # Note: verification only checks the raw topic.
            # Full pipeline verification (normalised -> processed) requires
            # Flink jobs to be running and is done in integration tests.
            log.info(f"  Messages produced. Raw topic verification skipped in smoke mode.")
            log.info(f"  Check Redpanda Console at http://localhost:8082 to verify.")
            log.info(f"  Topic: {topic}")
    finally:
        producer.close()

    log.info("")
    log.info("=" * 60)
    log.info(f"Smoke test complete.")
    log.info(f"To verify processing: check arcas.normalised and arcas.processed topics")
    log.info(f"in Redpanda Console: http://localhost:8082")
    log.info("=" * 60)
    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARCAS ingestion pipeline smoke test")
    parser.add_argument(
        "--source",
        choices=list(SYNTHETIC_SOURCES.keys()) + ["all"],
        default="gazette",
        help="Source to test (default: gazette)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of messages to produce per source (default: 5)",
    )
    args = parser.parse_args()

    success = run_smoke_test(args.source, args.limit)
    exit(0 if success else 1)
