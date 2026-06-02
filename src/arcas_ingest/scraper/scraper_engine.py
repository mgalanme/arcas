"""
ARCAS - src/arcas_ingest/scraper/scraper_engine.py  (v2 - fixed fact-checker URLs)

TWO categories of sources:
1. MEDIA_TARGETS (25 outlets): analysed for disinformation patterns
2. FACTCHECK_TARGETS (5 working outlets): used as verified truth sources

Produces to:
  arcas.raw.media      - media articles
  arcas.raw.factcheck  - fact-checker verdicts
"""
import hashlib, json, logging, os, re, time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_MEDIA      = "arcas.raw.media"
TOPIC_FACTCHECK  = "arcas.raw.factcheck"
REQUEST_DELAY    = 3.0
MAX_ARTICLES     = 30


@dataclass
class ScraperTarget:
    name:           str
    url:            str
    topic:          str = TOPIC_MEDIA
    use_js:         bool = False
    language:       str = "es"
    jurisdiction:   str = "ES"
    is_factchecker: bool = False
    verify_ssl:     bool = True


# =============================================================================
# MEDIA TARGETS - 25 outlets, full ideological spectrum
# =============================================================================
MEDIA_TARGETS = [
    ScraperTarget("El País",            "https://elpais.com/espana/"),
    ScraperTarget("El Mundo",           "https://www.elmundo.es/espana.html"),
    ScraperTarget("ABC",                "https://www.abc.es/espana/"),
    ScraperTarget("La Vanguardia",      "https://www.lavanguardia.com/politica"),
    ScraperTarget("El Confidencial",    "https://www.elconfidencial.com/espana/",   use_js=True),
    ScraperTarget("20 Minutos",         "https://www.20minutos.es/nacional/"),
    ScraperTarget("El Español",         "https://www.elespanol.com/espana/",        use_js=True),
    ScraperTarget("La Razón",           "https://www.larazon.es/espana/"),
    ScraperTarget("Público",            "https://www.publico.es/politica"),
    ScraperTarget("elDiario.es",        "https://www.eldiario.es/politica/",        use_js=True),
    ScraperTarget("OK Diario",          "https://okdiario.com/espana/",             use_js=True),
    ScraperTarget("infoLibre",          "https://www.infolibre.es/politica/",       use_js=True),
    ScraperTarget("El HuffPost",        "https://www.huffingtonpost.es/politica/",  use_js=True),
    ScraperTarget("Vozpópuli",          "https://www.vozpopuli.com/espana/",        use_js=True),
    ScraperTarget("Periodista Digital", "https://www.periodistadigital.com/"),
    ScraperTarget("Expansión",          "https://www.expansion.com/economia.html"),
    ScraperTarget("El Economista",      "https://www.eleconomista.es/economia/"),
    ScraperTarget("Cinco Días",         "https://cincodias.elpais.com/economia/"),
    ScraperTarget("El Correo",          "https://www.elcorreo.com/politica/"),
    ScraperTarget("La Nueva España",    "https://www.lne.es/espana/"),
    ScraperTarget("El Periódico",       "https://www.elperiodico.com/es/politica/", use_js=True),
    ScraperTarget("Ara",                "https://www.ara.cat/politica/",            language="ca"),
    ScraperTarget("Naiz",               "https://www.naiz.eus/eu/actualidad/",      language="eu"),
    ScraperTarget("AP News Spain",      "https://apnews.com/hub/spain",
                  language="en", jurisdiction="GL"),
    ScraperTarget("El Confidencial Legal", "https://www.elconfidencialegal.com/",  use_js=True),
    ScraperTarget("Confilegal",            "https://confilegal.com/",               use_js=False),
    ScraperTarget("Infojus Noticias",      "https://www.infojus.es/",               use_js=False),
    ScraperTarget("Diario Jurídico",       "https://www.diariojuridico.com/",       use_js=False),
    ScraperTarget("Transparency Int.",  "https://www.transparency.org/en/news",
                  language="en", jurisdiction="GL"),
]

# =============================================================================
# FACT-CHECKER TARGETS - verified truth sources for cross-referencing
# Results: Maldita(JS) + Newtral + Snopes + PolitiFact working
# VerificaRTVE: fixed URL below
# AFP Factual: fixed URL below
# EFE Verifica: SSL expired, verify_ssl=False
# =============================================================================
FACTCHECK_TARGETS = [
    ScraperTarget("Maldita.es",     "https://maldita.es/malditobulo/",
                  topic=TOPIC_FACTCHECK, use_js=True, is_factchecker=True),
    ScraperTarget("Newtral",        "https://www.newtral.es/zona-verificacion/fact-check/",
                  topic=TOPIC_FACTCHECK, is_factchecker=True),
    ScraperTarget("VerificaRTVE",   "https://www.rtve.es/noticias/verifica",
                  topic=TOPIC_FACTCHECK, is_factchecker=True),
    ScraperTarget("AFP Factual ES", "https://factual.afp.com/doc.afp.com",
                  topic=TOPIC_FACTCHECK, is_factchecker=True),
    ScraperTarget("EFE Verifica",   "https://efeverifica.com/",
                  topic=TOPIC_FACTCHECK, is_factchecker=True, verify_ssl=False),
    ScraperTarget("Snopes (EN)",    "https://www.snopes.com/fact-check/",
                  topic=TOPIC_FACTCHECK, is_factchecker=True,
                  language="en", jurisdiction="GL"),
    ScraperTarget("PolitiFact (EN)","https://www.politifact.com/factchecks/",
                  topic=TOPIC_FACTCHECK, is_factchecker=True,
                  language="en", jurisdiction="GL"),
]

ALL_TARGETS = MEDIA_TARGETS + FACTCHECK_TARGETS


class ScraperEngine:
    """
    Scrapes media outlets and fact-checkers.
    Media articles -> arcas.raw.media
    Fact-checker verdicts -> arcas.raw.factcheck

    The DisInfo Agent cross-references:
      - Claims in media articles
      - Verdicts in fact-checker records
      -> Identifies outlets repeating debunked claims
      -> Maps beneficiaries via the knowledge graph
    """

    def __init__(self, targets: list[ScraperTarget] = None):
        self.targets       = targets if targets is not None else ALL_TARGETS
        self.producer      = self._connect()
        self._seen_hashes: set[str] = set()

    def _connect(self) -> KafkaProducer:
        for attempt in range(10):
            try:
                p = KafkaProducer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                    key_serializer=lambda k: k.encode() if k else None,
                    acks="all", compression_type="gzip",
                )
                log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
                return p
            except NoBrokersAvailable:
                log.warning(f"Kafka not ready (attempt {attempt+1}/10)...")
                time.sleep(3)
        raise RuntimeError(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}")

    def _fetch_static(self, url: str, verify_ssl: bool = True) -> Optional[str]:
        try:
            r = httpx.get(
                url, timeout=20.0, follow_redirects=True,
                verify=verify_ssl,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ARCAS-Research/1.0)",
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                }
            )
            r.raise_for_status()
            return r.text
        except Exception as e:
            log.warning(f"Static fetch failed {url}: {e}")
            return None

    def _fetch_js(self, url: str) -> Optional[str]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(
                    user_agent="Mozilla/5.0 (compatible; ARCAS-Research/1.0)",
                    locale="es-ES",
                )
                page = ctx.new_page()
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                content = page.content()
                browser.close()
                return content
        except Exception as e:
            log.warning(f"JS fetch failed {url}: {e} - falling back to static")
            return self._fetch_static(url)

    def _extract_articles(self, html: str, target: ScraperTarget) -> list[dict]:
        articles = []
        seen_titles: set[str] = set()

        headings = re.findall(
            r'<h[123][^>]*>([^<]{30,300})</h[123]>',
            html, re.IGNORECASE | re.DOTALL
        )
        links = re.findall(
            r'href=["\']([^"\']{10,})["\'][^>]*>([^<]{40,250})</a>',
            html, re.IGNORECASE
        )

        candidates = []
        for h in headings:
            text = re.sub(r'\s+', ' ', h).strip()
            if len(text) >= 30 and text not in seen_titles:
                seen_titles.add(text)
                candidates.append((text, target.url))

        for href, text in links:
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) < 40 or text in seen_titles:
                continue
            seen_titles.add(text)
            if href.startswith('/'):
                base = '/'.join(target.url.split('/')[:3])
                href = base + href
            elif not href.startswith('http'):
                continue
            candidates.append((text, href))

        for title, url in candidates[:MAX_ARTICLES]:
            content_hash = hashlib.sha256(
                f"{title}|{target.name}".encode()
            ).hexdigest()
            if content_hash in self._seen_hashes:
                continue
            self._seen_hashes.add(content_hash)
            articles.append({
                "source_type":    "factcheck" if target.is_factchecker else "media",
                "source_name":    target.name,
                "is_factchecker": target.is_factchecker,
                "title":          title,
                "content_url":    url,
                "language":       target.language,
                "jurisdiction":   target.jurisdiction,
                "content_hash":   content_hash,
                "is_synthetic":   False,
            })

        return articles[:MAX_ARTICLES]

    def scrape_target(self, target: ScraperTarget) -> int:
        label = "fact-checker" if target.is_factchecker else "media"
        log.info(f"Scraping: {target.name} ({label})")
        html = (self._fetch_js(target.url) if target.use_js
                else self._fetch_static(target.url, target.verify_ssl))
        if not html:
            log.warning(f"  No content from {target.name}")
            return 0
        articles = self._extract_articles(html, target)
        for article in articles:
            self.producer.send(target.topic, key=article["content_hash"], value=article)
        self.producer.flush()
        log.info(f"  Produced {len(articles)} records from {target.name}")
        return len(articles)

    def scrape_all(self) -> dict:
        media_total = factcheck_total = 0
        for i, target in enumerate(self.targets):
            label = "FC" if target.is_factchecker else "M"
            log.info(f"[{i+1}/{len(self.targets)}][{label}] {target.name}")
            count = self.scrape_target(target)
            if target.is_factchecker:
                factcheck_total += count
            else:
                media_total += count
            time.sleep(REQUEST_DELAY)
        return {"media": media_total, "factcheck": factcheck_total,
                "total": media_total + factcheck_total}

    def scrape_media_only(self) -> int:
        self.targets = MEDIA_TARGETS
        return self.scrape_all()["total"]

    def scrape_factcheckers_only(self) -> int:
        self.targets = FACTCHECK_TARGETS
        return self.scrape_all()["total"]

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["all", "media", "factcheck"], default="all")
    parser.add_argument("--outlets", help="Comma-separated outlet names")
    args = parser.parse_args()
    engine = ScraperEngine()
    try:
        if args.outlets:
            names = [n.strip() for n in args.outlets.split(",")]
            engine.targets = [t for t in ALL_TARGETS if t.name in names]
            engine.scrape_all()
        elif args.mode == "media":
            engine.scrape_media_only()
        elif args.mode == "factcheck":
            engine.scrape_factcheckers_only()
        else:
            results = engine.scrape_all()
            log.info(f"Final: {results['media']} media + {results['factcheck']} fact-check records")
    finally:
        engine.close()
