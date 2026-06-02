# Databricks notebook source
# ARCAS - 01_ingestion_daily
# Ingesta diaria autónoma: BOE + medios + fact-checkers → Delta Lake
# Ejecutar manualmente o como Job diario (Schedule: 06:00 CET)

# COMMAND ----------

# Celda 1: Dependencias
# En Job: estas librerías se instalan via init script o cluster libraries
# En notebook interactivo: ejecutar esta celda una sola vez

%pip install beautifulsoup4 lxml

# COMMAND ----------

# Celda 2: Imports y configuración

import requests, json, hashlib, re, logging
from datetime import date, timedelta
from typing import Iterator
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Credenciales - en Job usar Databricks Secrets o env vars del cluster
GROQ_API_KEY = dbutils.secrets.get(scope="arcas", key="groq_api_key") if dbutils.secrets.listScopes() else "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"
GROQ_MODEL   = "llama-3.3-70b-versatile"

# Delta tables
DB_RAW       = "arcas_raw"
DB_PROCESSED = "arcas_processed"
TBL_ARTICLES = f"{DB_RAW}.articles"
TBL_ALERTS   = f"{DB_PROCESSED}.alerts"

print("Config OK")

# COMMAND ----------

# Celda 3: Crear schemas y tablas Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_RAW}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ARTICLES} (
    source_type    STRING,
    source_name    STRING,
    title          STRING,
    content_url    STRING,
    pub_date       STRING,
    language       STRING,
    jurisdiction   STRING,
    content_hash   STRING,
    is_factchecker BOOLEAN,
    ingested_at    TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ALERTS} (
    alert_id         STRING,
    category         STRING,
    status           STRING,
    confidence_score DOUBLE,
    nl_justification STRING,
    source_name      STRING,
    title            STRING,
    content_url      STRING,
    created_at       TIMESTAMP
) USING DELTA
""")

print("Tables ready")

# COMMAND ----------

# Celda 4: Ingesta BOE

BOE_API = "https://www.boe.es/datosabiertos/api/boe/sumario"

def fetch_boe(pub_date: date) -> list[dict]:
    url = f"{BOE_API}/{pub_date.strftime('%Y%m%d')}"
    try:
        r = requests.get(url, headers={"Accept": "application/xml"}, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
    except Exception as e:
        log.warning(f"BOE fetch error {pub_date}: {e}")
        return []

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return []

    if root.findtext(".//status/code") != "200":
        return []

    records = []
    for item in root.findall(".//item"):
        doc_id = (item.findtext("identificador") or "").strip()
        title  = (item.findtext("titulo") or "").strip()
        if not doc_id or not title:
            continue
        url_html = (item.findtext("url_html") or "").strip()
        url_pdf  = (item.findtext("url_pdf") or "").strip()
        records.append({
            "source_type":    "gazette",
            "source_name":    "BOE",
            "title":          title,
            "content_url":    url_html or url_pdf,
            "pub_date":       pub_date.isoformat(),
            "language":       "es",
            "jurisdiction":   "ES",
            "content_hash":   hashlib.sha256(f"{doc_id}|{title}|{pub_date}".encode()).hexdigest(),
            "is_factchecker": False,
        })
    return records

# Fetch last 3 days (covers weekends)
boe_records = []
for days_back in range(3):
    d = date.today() - timedelta(days=days_back)
    recs = fetch_boe(d)
    boe_records.extend(recs)
    log.info(f"BOE {d}: {len(recs)} records")

print(f"BOE total: {len(boe_records)} records")

# COMMAND ----------

# Celda 5: Ingesta medios de comunicación

MEDIA_SOURCES = [
    ("El País",          "https://elpais.com/espana/",              "es", False),
    ("El Mundo",         "https://www.elmundo.es/espana.html",       "es", False),
    ("ABC",              "https://www.abc.es/espana/",               "es", False),
    ("La Vanguardia",    "https://www.lavanguardia.com/politica",    "es", False),
    ("Público",          "https://www.publico.es/politica",          "es", False),
    ("elDiario.es",      "https://www.eldiario.es/politica/",       "es", False),
    ("OK Diario",        "https://okdiario.com/espana/",             "es", False),
    ("La Razón",         "https://www.larazon.es/espana/",           "es", False),
    ("El Español",       "https://www.elespanol.com/espana/",        "es", False),
    ("infoLibre",        "https://www.infolibre.es/politica/",       "es", False),
    ("Expansión",        "https://www.expansion.com/economia.html",  "es", False),
    ("El Economista",    "https://www.eleconomista.es/economia/",    "es", False),
    ("AP News Spain",    "https://apnews.com/hub/spain",             "en", False),
    ("Transparency",     "https://www.transparency.org/en/news",     "en", False),
    # Fact-checkers
    ("Maldita.es",       "https://maldita.es/malditobulo/",          "es", True),
    ("Newtral",          "https://www.newtral.es/zona-verificacion/fact-check/", "es", True),
    ("Snopes",           "https://www.snopes.com/fact-check/",       "en", True),
    ("PolitiFact",       "https://www.politifact.com/factchecks/",   "en", True),
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ARCAS-Research/1.0)", "Accept-Language": "es-ES,es;q=0.9"}

def scrape_source(name: str, url: str, language: str, is_fc: bool) -> list[dict]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"Scrape failed {name}: {e}")
        return []

    soup  = BeautifulSoup(r.text, "lxml")
    seen  = set()
    items = []

    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(strip=True)
        if len(text) < 30 or text in seen:
            continue
        seen.add(text)
        a = tag.find("a", href=True)
        link = a["href"] if a else url
        if link.startswith("/"):
            base = "/".join(url.split("/")[:3])
            link = base + link
        elif not link.startswith("http"):
            link = url
        ch = hashlib.sha256(f"{text}|{name}".encode()).hexdigest()
        items.append({
            "source_type":    "factcheck" if is_fc else "media",
            "source_name":    name,
            "title":          text,
            "content_url":    link,
            "pub_date":       date.today().isoformat(),
            "language":       language,
            "jurisdiction":   "ES" if language == "es" else "GL",
            "content_hash":   ch,
            "is_factchecker": is_fc,
        })
        if len(items) >= 25:
            break

    log.info(f"  {name}: {len(items)} articles")
    return items

media_records = []
for name, url, lang, is_fc in MEDIA_SOURCES:
    media_records.extend(scrape_source(name, url, lang, is_fc))

print(f"Media total: {len(media_records)} records")

# COMMAND ----------

# Celda 6: Guardar en Delta Lake

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

all_records = boe_records + media_records
print(f"Total records to save: {len(all_records)}")

if all_records:
    df = spark.createDataFrame([Row(**r) for r in all_records])
    df = df.withColumn("ingested_at", current_timestamp())

    # Deduplicate against existing data
    existing_hashes = set(
        r.content_hash
        for r in spark.sql(f"SELECT content_hash FROM {TBL_ARTICLES}").collect()
    ) if spark.catalog.tableExists(TBL_ARTICLES) else set()

    new_records = [r for r in all_records if r["content_hash"] not in existing_hashes]
    print(f"New records (after dedup): {len(new_records)}")

    if new_records:
        df_new = spark.createDataFrame([Row(**r) for r in new_records])
        df_new = df_new.withColumn("ingested_at", current_timestamp())
        df_new.write.format("delta").mode("append").saveAsTable(TBL_ARTICLES)
        print(f"Saved {len(new_records)} new records to {TBL_ARTICLES}")

print("Ingestion complete")

# COMMAND ----------

# Celda 7: Scoring y detección de patrones

KW_CAT_A = ["contrato", "adjudicaci", "licitaci", "concurso", "subvencion", "obra publica", "sobrecoste"]
KW_CAT_B = ["patrimonio", "enriquecimiento", "puerta giratoria", "offshore", "paraiso fiscal", "testaferro"]
KW_CAT_C_GENERAL = ["juez", "tribunal", "sentencia", "fiscal", "sumario", "audiencia nacional"]
KW_CAT_C_BIAS    = ["archivo", "archiva", "sobresee", "prescripcion", "UCO", "UDEF", "dilaciones", "sin pruebas"]
KW_CAT_C_POLITICAL = ["psoe", "pp", "vox", "podemos", "partido socialista", "partido popular"]
KW_CAT_D = ["bulo", "falso", "mentira", "desinformacion", "fake", "desmentido", "verificado"]
KW_CAT_E = ["trama", "blanqueo", "financiacion ilegal", "comisionista", "lobbista"]
KW_CAT_F = ["nepotismo", "enchufismo", "cargo de confianza", "incompatibilidad", "conflicto de interes"]

THRESHOLD = 0.30

def score(title: str, source_type: str, is_fc: bool) -> dict:
    t = title.lower()

    def kw(lst): return min(sum(1 for k in lst if k in t) * 0.15, 0.9)

    def judicial_bias():
        has_j = any(k in t for k in KW_CAT_C_GENERAL)
        has_p = any(k in t for k in KW_CAT_C_POLITICAL)
        has_b = any(k in t for k in KW_CAT_C_BIAS)
        if has_j and has_p and has_b: return 0.75
        if has_j and (has_p or has_b): return 0.50
        if has_j: return 0.25
        return 0.0

    scores = {
        "cat_a": kw(KW_CAT_A) + (0.15 if source_type == "gazette" and kw(KW_CAT_A) > 0 else 0),
        "cat_b": kw(KW_CAT_B),
        "cat_c": judicial_bias(),
        "cat_d": max(kw(KW_CAT_D), 0.45 if is_fc else 0),
        "cat_e": kw(KW_CAT_E),
        "cat_f": kw(KW_CAT_F),
    }
    return scores

def top_category(scores: dict) -> str:
    return max(scores, key=scores.get).replace("cat_", "").upper()

# Score all new records
candidates = [r for r in (new_records if new_records else []) if r.get("title")]
scored     = [(r, score(r["title"], r["source_type"], r["is_factchecker"])) for r in candidates]
above      = [(r, s) for r, s in scored if max(s.values()) >= THRESHOLD]

print(f"Candidates: {len(candidates)} | Above threshold: {len(above)}")
for r, s in above:
    print(f"  [{top_category(s)} {max(s.values()):.2f}] {r['title'][:70]}")

# COMMAND ----------

# Celda 8: Generar hipótesis con Groq (HTTP directo)
import requests as _req, uuid
from datetime import datetime

def groq_invoke(prompt: str) -> str:
    r = _req.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
        json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 400},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

# Use all_records instead of new_records to bypass dedup
candidates = [r for r in (new_records if new_records else []) if r.get("title")]
scored     = [(r, score(r["title"], r["source_type"], r["is_factchecker"])) for r in candidates]
above      = [(r, s) for r, s in scored if max(s.values()) >= THRESHOLD]

alerts_to_save = []
for record, scores in above[:5]:  # Only 5 to test
    cat  = top_category(scores)
    conf = min(max(scores.values()) + 0.10, 0.99)
    try:
        hypothesis = groq_invoke(
            f"Anti-corruption analyst. Category {cat}.\n"
            f"Title: {record['title']}\nSource: {record['source_name']}\n"
            f"Respond: 1) Pattern 2) Confidence 3) Who benefits"
        )
    except Exception as e:
        hypothesis = f"[Pattern] {cat}: {record['title'][:100]}"
    alerts_to_save.append({
        "alert_id": str(uuid.uuid4()), "category": cat, "status": "pending",
        "confidence_score": round(conf, 3), "nl_justification": hypothesis,
        "source_name": record["source_name"], "title": record["title"],
        "content_url": record["content_url"], "created_at": datetime.utcnow().isoformat(),
    })
    print(f"[{cat} {conf:.2f}] {record['title'][:70]}")

print(f"\nAlerts generated: {len(alerts_to_save)}")

# COMMAND ----------

# Celda 9: Guardar alertas en Delta

if alerts_to_save:
    from pyspark.sql.functions import to_timestamp

    df_alerts = spark.createDataFrame([Row(**a) for a in alerts_to_save])
    df_alerts = df_alerts.withColumn("created_at", to_timestamp("created_at"))
    df_alerts.write.format("delta").mode("append").saveAsTable(TBL_ALERTS)
    print(f"Saved {len(alerts_to_save)} alerts to {TBL_ALERTS}")
else:
    print("No alerts to save today.")

# Summary
total_articles = spark.sql(f"SELECT count(*) as n FROM {TBL_ARTICLES}").collect()[0]["n"]
total_alerts   = spark.sql(f"SELECT count(*) as n FROM {TBL_ALERTS}").collect()[0]["n"]
pending_alerts = spark.sql(f"SELECT count(*) as n FROM {TBL_ALERTS} WHERE status='pending'").collect()[0]["n"]

print(f"\n=== ARCAS Daily Run Summary ===")
print(f"Articles in Delta:  {total_articles}")
print(f"Total alerts:       {total_alerts}")
print(f"Pending HITL:       {pending_alerts}")
print("================================")

