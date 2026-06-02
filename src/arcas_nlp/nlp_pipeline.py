"""
ARCAS - src/arcas_nlp/nlp_pipeline.py

NLP pipeline: reads from raw Kafka topics (gazette, media, factcheck),
applies NER + embedding generation, produces to arcas.nlp.extracted.

In production this reads from arcas.normalised (after Flink normalisation).
For Phase 1, reads directly from raw topics to avoid Flink dependency.
"""
import json, logging, os, time
from typing import Optional

import spacy
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_IN         = os.getenv("KAFKA_TOPIC_IN", "arcas.normalised")
TOPIC_OUT        = "arcas.nlp.extracted"
CONSUMER_GROUP   = "arcas-nlp-pipeline"
EMBEDDING_MODEL  = os.getenv("EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1")
EMBEDDING_DIMS   = int(os.getenv("EMBEDDING_DIMS", "768"))
HF_TOKEN         = os.getenv("HF_TOKEN", "")


class NLPPipeline:
    """
    Processes raw/normalised records through:
    1. spaCy NER (Spanish + English)
    2. Embedding generation (nomic-embed-text-v1, 768 dims)
    3. Produces enriched records to arcas.nlp.extracted
    """

    def __init__(self):
        log.info("Loading spaCy models...")
        self.nlp_es = spacy.load("es_core_news_lg")
        self.nlp_en = spacy.load("en_core_web_lg")
        log.info("Loading embedding model...")
        self._load_embedder()
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()
        log.info("NLP pipeline ready.")

    def _load_embedder(self):
        from sentence_transformers import SentenceTransformer
        # trust_remote_code=True is required by nomic-embed-text-v1
        # which uses custom pooling code hosted on HuggingFace.
        # The model is well-known and safe; this is a HF security prompt only.
        token = HF_TOKEN if HF_TOKEN and not HF_TOKEN.startswith("hf_YOUR") else None
        self.embedder = SentenceTransformer(
            EMBEDDING_MODEL,
            trust_remote_code=True,
            token=token,
        )
        log.info(f"Embedder loaded: {EMBEDDING_MODEL} ({EMBEDDING_DIMS} dims)")

    def _create_consumer(self):
        for i in range(10):
            try:
                return KafkaConsumer(
                    TOPIC_IN,
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    group_id=CONSUMER_GROUP,
                    auto_offset_reset="earliest",
                    value_deserializer=lambda v: json.loads(v.decode()),
                    consumer_timeout_ms=10000,
                )
            except NoBrokersAvailable:
                time.sleep(3)
        raise RuntimeError("Cannot connect to Kafka")

    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks="all", compression_type="gzip",
        )

    def _extract_entities(self, text: str, language: str) -> list[dict]:
        nlp  = self.nlp_es if language == "es" else self.nlp_en
        doc  = nlp(text[:10000])
        seen = set()
        entities = []
        for ent in doc.ents:
            key = f"{ent.text.strip().lower()}|{ent.label_}"
            if key in seen:
                continue
            seen.add(key)
            entities.append({
                "text":  ent.text.strip(),
                "label": ent.label_,
            })
        return entities

    def _actor_type(self, label: str) -> str:
        return {
            "PER":    "other",
            "PERSON": "other",
            "ORG":    "corporate",
            "NORP":   "political",
            "GPE":    "other",
        }.get(label, "other")

    def _embed(self, text: str) -> list[float]:
        return self.embedder.encode(
            text[:2048],
            normalize_embeddings=True,
        ).tolist()

    def process_record(self, record: dict) -> Optional[dict]:
        text     = record.get("title") or record.get("pseudonymised_text") or ""
        language = record.get("language", "es")
        if not text or len(text) < 15:
            return None

        entities = self._extract_entities(text, language)
        proposals = [
            {
                "surface_form":    e["text"],
                "ner_label":       e["label"],
                "actor_type_hint": self._actor_type(e["label"]),
            }
            for e in entities
            if e["label"] in {"PER", "PERSON", "ORG", "NORP"}
        ]

        return {
            **record,
            "entities":          entities,
            "entity_proposals":  proposals,
            "embedding_vector":  self._embed(text),
            "embedding_dims":    EMBEDDING_DIMS,
            "nlp_processed":     True,
        }

    def run(self, max_records: int = 0):
        log.info(f"Pipeline running on topic: {TOPIC_IN} (max={max_records or 'unlimited'})")
        processed = 0
        for message in self.consumer:
            record   = message.value
            enriched = self.process_record(record)
            if enriched:
                key = record.get("content_hash")
                self.producer.send(TOPIC_OUT, key=key, value=enriched)
                processed += 1
                if processed % 10 == 0:
                    log.info(f"  Processed {processed} records")
                    self.producer.flush()
            if max_records and processed >= max_records:
                break
        self.producer.flush()
        log.info(f"Pipeline stopped. Total processed: {processed}")
        return processed

    def close(self):
        self.consumer.close()
        self.producer.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic",   default="arcas.raw.gazette",
                        help="Input Kafka topic (default: arcas.raw.gazette)")
    parser.add_argument("--limit",   type=int, default=0,
                        help="Max records to process (0=unlimited)")
    args = parser.parse_args()
    os.environ["KAFKA_TOPIC_IN"] = args.topic

    pipeline = NLPPipeline()
    try:
        pipeline.run(max_records=args.limit)
    finally:
        pipeline.close()
