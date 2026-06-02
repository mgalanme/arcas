"""
ARCAS - src/arcas_ingest/connectors/gazette/boe_connector.py

BOE (Boletín Oficial del Estado) connector.
Uses the official open data API: https://www.boe.es/datosabiertos/api/boe/sumario/YYYYMMDD
Returns XML with full publication structure.
Produces to topic: arcas.raw.gazette
"""
import hashlib, json, logging, os, time, xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Iterator

import httpx
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC             = "arcas.raw.gazette"
BOE_API_BASE      = "https://www.boe.es/datosabiertos/api/boe/sumario"
REQUEST_DELAY_SEC = 1.0
MAX_RETRIES       = 3


class BOEConnector:
    """
    Fetches BOE daily summary via the official open data API and produces
    to Kafka. Each message contains: source_type, pub_date, document_id,
    title, department, section, content_url, content_hash, is_synthetic=False.
    """

    def __init__(self):
        self.client   = httpx.Client(
            timeout=30.0,
            headers={"Accept": "application/xml", "User-Agent": "ARCAS-Research/1.0"},
            follow_redirects=True,
        )
        self.producer = self._connect()

    def _connect(self):
        for attempt in range(10):
            try:
                p = KafkaProducer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                    key_serializer=lambda k: k.encode() if k else None,
                    acks="all",
                    compression_type="gzip",
                )
                log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
                return p
            except NoBrokersAvailable:
                log.warning(f"Kafka not ready (attempt {attempt+1}/10)...")
                time.sleep(3)
        raise RuntimeError(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}")

    def _fetch(self, d: date) -> str | None:
        """Fetch the BOE daily summary XML for a given date."""
        url = f"{BOE_API_BASE}/{d.strftime('%Y%m%d')}"
        for i in range(1, MAX_RETRIES + 1):
            try:
                r = self.client.get(url)
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                # The API wraps XML inside a <response><data> envelope
                return r.text
            except httpx.HTTPStatusError as e:
                log.warning(f"HTTP {e.response.status_code} attempt {i} for {d}")
            except httpx.RequestError as e:
                log.warning(f"Request error attempt {i}: {e}")
            time.sleep(2 ** i)
        return None

    def _parse(self, xml_content: str, d: date) -> Iterator[dict]:
        """
        Parse the BOE open data API response.
        Structure: response/data/sumario/diario/seccion/departamento/epigrafe/item
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            log.error(f"XML parse error: {e}")
            return

        # Check API status
        code = root.findtext(".//status/code", "")
        if code != "200":
            log.info(f"  API returned status {code} for {d} - no BOE published")
            return

        for item in root.findall(".//item"):
            doc_id = (item.findtext("identificador") or "").strip()
            title  = (item.findtext("titulo") or "").strip()
            if not doc_id or not title:
                continue

            # Navigate up to get section and department names
            url_html = (item.findtext("url_html") or "").strip()
            url_pdf  = (item.findtext("url_pdf")  or "").strip()

            yield {
                "source_type":  "gazette",
                "source_name":  "BOE",
                "pub_date":     d.isoformat(),
                "document_id":  doc_id,
                "title":        title,
                "content_url":  url_html or url_pdf,
                "jurisdiction": "ES",
                "language":     "es",
                "content_hash": hashlib.sha256(
                    f"{doc_id}|{title}|{d}".encode()
                ).hexdigest(),
                "is_synthetic": False,
            }

    def fetch_date(self, d: date) -> int:
        log.info(f"Fetching BOE {d.isoformat()}")
        xml = self._fetch(d)
        if not xml:
            log.info(f"  No BOE for {d} (weekend, holiday or not yet published)")
            return 0
        count = 0
        for record in self._parse(xml, d):
            self.producer.send(TOPIC, key=record["content_hash"], value=record)
            count += 1
        self.producer.flush()
        log.info(f"  Produced {count} records for {d.isoformat()}")
        return count

    def fetch_range(self, start: date, end: date) -> int:
        total, cur = 0, start
        while cur <= end:
            total += self.fetch_date(cur)
            cur += timedelta(days=1)
            time.sleep(REQUEST_DELAY_SEC)
        return total

    def close(self):
        self.producer.close()
        self.client.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="ARCAS BOE Connector")
    parser.add_argument("--date",      help="Specific date YYYY-MM-DD (default: today)")
    parser.add_argument("--days-back", type=int, default=1, help="Days back from today")
    args = parser.parse_args()
    connector = BOEConnector()
    try:
        if args.date:
            connector.fetch_date(date.fromisoformat(args.date))
        else:
            end = date.today()
            connector.fetch_range(end - timedelta(days=args.days_back - 1), end)
    finally:
        connector.close()
