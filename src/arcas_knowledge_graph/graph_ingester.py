"""
ARCAS - src/arcas_knowledge_graph/graph_ingester.py

Consumes from arcas.nlp.extracted and writes to:
  - Neo4j: Actor nodes with type hints from NER
  - Qdrant: Embedding vectors for semantic search

This is the bridge between the NLP pipeline and the knowledge stores.
It runs alongside the NLP pipeline as a separate consumer.
"""
import json, logging, os, time, uuid
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, UpdateStatus
)

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_IN         = "arcas.nlp.extracted"
CONSUMER_GROUP   = "arcas-graph-ingester"
NEO4J_URI        = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER       = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD   = os.getenv("NEO4J_PASSWORD", "")
QDRANT_HOST      = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT      = int(os.getenv("QDRANT_PORT", "6333"))
EMBEDDING_DIMS   = int(os.getenv("EMBEDDING_DIMS", "768"))
COLLECTION_NAME  = "arcas_evidence"


class GraphIngester:
    """
    Reads NLP-enriched records and persists:
    1. Actor proposals -> Neo4j Actor nodes (upsert by surface_form)
    2. Evidence + embedding -> Qdrant collection arcas_evidence
    3. Source nodes -> Neo4j Source nodes linked to evidence
    """

    def __init__(self):
        self.neo4j    = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        self.qdrant   = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        self.consumer = self._create_consumer()
        self._ensure_qdrant_collection()
        log.info("Graph ingester ready.")

    def _create_consumer(self):
        for attempt in range(10):
            try:
                return KafkaConsumer(
                    TOPIC_IN,
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    group_id=CONSUMER_GROUP,
                    auto_offset_reset="earliest",
                    value_deserializer=lambda v: json.loads(v.decode()),
                    consumer_timeout_ms=15000,
                )
            except NoBrokersAvailable:
                log.warning(f"Kafka not ready ({attempt+1}/10)...")
                time.sleep(3)
        raise RuntimeError("Cannot connect to Kafka")

    def _ensure_qdrant_collection(self):
        """Create the Qdrant collection if it doesn't exist."""
        existing = [c.name for c in self.qdrant.get_collections().collections]
        if COLLECTION_NAME not in existing:
            self.qdrant.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIMS,
                    distance=Distance.COSINE,
                ),
            )
            log.info(f"Qdrant collection '{COLLECTION_NAME}' created.")
        else:
            log.info(f"Qdrant collection '{COLLECTION_NAME}' already exists.")

    def _upsert_actor(self, surface_form: str, actor_type: str,
                      source_name: str) -> str:
        """
        Upsert an Actor node in Neo4j.
        Uses surface_form as display_name for now (pseudonymisation
        is applied in production via the vault service).
        Returns the actor_id.
        """
        actor_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, surface_form.lower()))
        with self.neo4j.session() as s:
            s.run("""
                MERGE (a:Actor {actor_id: $actor_id})
                ON CREATE SET
                    a.display_name     = $display_name,
                    a.actor_type       = $actor_type,
                    a.first_detected   = datetime(),
                    a.last_updated     = datetime(),
                    a.risk_score_global = 0.0,
                    a.sources          = [$source_name]
                ON MATCH SET
                    a.last_updated     = datetime(),
                    a.sources          = CASE
                        WHEN NOT $source_name IN a.sources
                        THEN a.sources + [$source_name]
                        ELSE a.sources
                    END
            """, actor_id=actor_id, display_name=surface_form,
                 actor_type=actor_type, source_name=source_name)
        return actor_id

    def _upsert_source(self, source_name: str, source_type: str) -> None:
        """Upsert a Source node in Neo4j."""
        with self.neo4j.session() as s:
            s.run("""
                MERGE (s:Source {source_name: $source_name})
                ON CREATE SET
                    s.source_type  = $source_type,
                    s.first_seen   = datetime(),
                    s.article_count = 1
                ON MATCH SET
                    s.article_count = s.article_count + 1,
                    s.last_seen     = datetime()
            """, source_name=source_name, source_type=source_type)

    def _store_embedding(self, record: dict, actor_ids: list[str]) -> str:
        """Store the embedding vector in Qdrant. Returns point_id."""
        point_id = str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            record.get("content_hash", str(uuid.uuid4()))
        ))
        self.qdrant.upsert(
            collection_name=COLLECTION_NAME,
            points=[PointStruct(
                id=point_id,
                vector=record["embedding_vector"],
                payload={
                    "source_name":  record.get("source_name", ""),
                    "source_type":  record.get("source_type", ""),
                    "title":        record.get("title", ""),
                    "content_url":  record.get("content_url", ""),
                    "pub_date":     record.get("pub_date", ""),
                    "language":     record.get("language", "es"),
                    "actor_ids":    actor_ids,
                    "is_factchecker": record.get("is_factchecker", False),
                    "content_hash": record.get("content_hash", ""),
                }
            )]
        )
        return point_id

    def process_record(self, record: dict) -> dict:
        """Process one NLP-enriched record. Returns ingestion summary."""
        source_name  = record.get("source_name", "unknown")
        source_type  = record.get("source_type", "media")
        proposals    = record.get("entity_proposals", [])
        embedding    = record.get("embedding_vector", [])

        # 1. Upsert source node
        self._upsert_source(source_name, source_type)

        # 2. Upsert actor nodes for all entity proposals
        actor_ids = []
        for proposal in proposals:
            actor_id = self._upsert_actor(
                surface_form=proposal["surface_form"],
                actor_type=proposal["actor_type_hint"],
                source_name=source_name,
            )
            actor_ids.append(actor_id)

        # 3. Store embedding in Qdrant
        point_id = None
        if embedding and len(embedding) == EMBEDDING_DIMS:
            point_id = self._store_embedding(record, actor_ids)

        return {
            "source":    source_name,
            "actors":    len(actor_ids),
            "embedded":  point_id is not None,
        }

    def run(self, max_records: int = 0) -> int:
        log.info(f"Graph ingester running (max={max_records or 'unlimited'})")
        processed = 0
        for message in self.consumer:
            record = message.value
            if not record.get("nlp_processed"):
                continue
            summary = self.process_record(record)
            processed += 1
            if processed % 10 == 0:
                log.info(f"  Ingested {processed}: {summary}")
            if max_records and processed >= max_records:
                break
        log.info(f"Graph ingester stopped. Total: {processed}")
        return processed

    def close(self):
        self.consumer.close()
        self.neo4j.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(description="ARCAS Graph Ingester")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max records to ingest (0=unlimited)")
    args = parser.parse_args()

    ingester = GraphIngester()
    try:
        ingester.run(max_records=args.limit)
    finally:
        ingester.close()
