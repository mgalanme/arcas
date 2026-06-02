"""
ARCAS - src/arcas_ingest/connectors/procurement/placsp_connector.py

PLACSP (Plataforma de Contratacion del Sector Publico) connector.
Fetches public contract data via the PLACSP Atom/RSS feeds and open data API.
Produces to topic: arcas.raw.procurement

API: https://contrataciondelestado.es/wps/portal/plataforma
Open data feeds: https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx
"""
import hashlib, json, logging, os, time
from datetime import date, timedelta
from typing import Iterator
import httpx, feedparser
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC               = "arcas.raw.procurement"
# PLACSP Atom feed - today's contracts
PLACSP_ATOM_BASE    = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteEconomico3.atom"
REQUEST_DELAY_SEC   = 2.0


class PLACSPConnector:
    """
    Fetches public contracts from PLACSP and produces to Kafka.
    Each message includes: contract_id, title, awarding_body, amount,
    procedure_type, cpv_codes, award_date, is_synthetic=False.
    """

    def __init__(self):
        self.client   = httpx.Client(timeout=30.0, headers={"User-Agent": "ARCAS-Research/1.0"})
        self.producer = self._connect()

    def _connect(self):
        for i in range(10):
            try:
                return KafkaProducer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                    key_serializer=lambda k: k.encode() if k else None,
                    acks="all", compression_type="gzip",
                )
            except NoBrokersAvailable:
                time.sleep(3)
        raise RuntimeError("Cannot connect to Kafka")

    def _fetch_feed(self, url: str) -> list[dict]:
        """Parse an Atom feed and return normalised contract records."""
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            log.error(f"Feed parse error: {e}")
            return []

        records = []
        for entry in feed.entries:
            contract_id = (getattr(entry, "id", "") or "").strip()
            title       = (getattr(entry, "title", "") or "").strip()
            summary     = (getattr(entry, "summary", "") or "").strip()
            pub_date    = (getattr(entry, "published", "") or date.today().isoformat())[:10]
            link        = (getattr(entry, "link", "") or "").strip()

            if not contract_id or not title:
                continue

            content_hash = hashlib.sha256(f"{contract_id}|{title}|{pub_date}".encode()).hexdigest()

            records.append({
                "source_type":    "procurement",
                "source_name":    "PLACSP",
                "pub_date":       pub_date,
                "contract_id":    contract_id,
                "title":          title,
                "summary":        summary,
                "content_url":    link,
                "jurisdiction":   "ES",
                "language":       "es",
                "content_hash":   content_hash,
                "is_synthetic":   False,
            })
        return records

    def fetch_latest(self) -> int:
        """Fetch and produce the latest contracts from the PLACSP Atom feed."""
        log.info("Fetching PLACSP latest contracts")
        records = self._fetch_feed(PLACSP_ATOM_BASE)
        count = 0
        for record in records:
            self.producer.send(TOPIC, key=record["content_hash"], value=record)
            count += 1
        self.producer.flush()
        log.info(f"  Produced {count} procurement records")
        return count

    def close(self):
        self.producer.close()
        self.client.close()
