# Databricks notebook source
# ARCAS - 01_ingestion_daily v2
# Ingesta diaria autónoma: BOE + medios + fact-checkers + fuentes judiciales → Delta Lake
# Cambios v2:
#   - Nuevas fuentes: Poder Judicial, Transparencia, Civio, EFE Verifica, RTVE Verifica
#   - Traducción al castellano de títulos en inglés via Groq
#   - Keywords mejorados para sesgo judicial, desinformación y redes de influencia
#   - Prompt de análisis en español

# COMMAND ----------
# Celda 1: Título y descripción (sin código)
# ARCAS v2 — ingesta diaria ampliada

# COMMAND ----------
# Celda 2: Imports y configuración

import requests, json, hashlib, re, logging, time
from datetime import date, timedelta
from typing import Iterator
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

try:
    GROQ_API_KEY = dbutils.secrets.get(scope="arcas", key="groq_api_key")
except Exception:
    try:
        GROQ_API_KEY = dbutils.widgets.get("GROQ_API_KEY")
    except Exception:
        GROQ_API_KEY = ""

GROQ_MODEL = "llama-3.3-70b-versatile"

DB_RAW       = "arcas_raw"
DB_PROCESSED = "arcas_processed"
TBL_ARTICLES = f"{DB_RAW}.articles"
TBL_ALERTS   = f"{DB_PROCESSED}.alerts"

print(f"Config OK — GROQ key present: {bool(GROQ_API_KEY)}")

# COMMAND ----------
# Celda 3: Groq helper

def groq_invoke(prompt: str, max_tokens: int = 400, temperature: float = 0.1) -> str:
    """Call Groq API with retry on rate limit."""
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": GROQ_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": max_tokens,
                      "temperature": temperature},
                timeout=30,
            )
            if r.status_code == 429:
                wait = 20 * (attempt + 1)
                log.warning(f"Groq rate limit — waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            log.warning(f"Groq attempt {attempt+1} failed: {e}")
            time.sleep(5)
    return ""

def translate_to_spanish(title: str) -> str:
    """Translate a title to Spanish if it appears to be in another language."""
    # Simple heuristic: if common Spanish words are absent, translate
    spanish_markers = ["el ", "la ", "los ", "las ", "de ", "del ", "en ", "por ", "que ", "con ", "una ", "un "]
    t_lower = title.lower()
    if any(m in t_lower for m in spanish_markers):
        return title  # Already Spanish
    result = groq_invoke(
        f"Traduce este titular al español. Devuelve solo el titular traducido, sin explicaciones:\n{title}",
        max_tokens=100,
        temperature=0.0,
    )
    return result if result else title

print("Groq helpers OK")

# COMMAND ----------
# Celda 4: Crear schemas y tablas Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_RAW}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ARTICLES} (
    source_type    STRING,
    source_name    STRING,
    title          STRING,
    title_es       STRING,
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

# Add title_es column if it doesn't exist yet (idempotent migration)
try:
    spark.sql(f"ALTER TABLE {TBL_ARTICLES} ADD COLUMN title_es STRING")
    print("Added title_es column")
except Exception:
    print("title_es column already exists")

print("Tables ready")

# COMMAND ----------
# Celda 5: Fuentes de ingesta

MEDIA_SOURCES = [
    # Prensa generalista — espectro ideológico completo
    ("El País",        "https://elpais.com/espana/",               "es", False),
    ("El Mundo",       "https://www.elmundo.es/espana.html",        "es", False),
    ("ABC",            "https://www.abc.es/espana/",                "es", False),
    ("La Vanguardia",  "https://www.lavanguardia.com/politica",     "es", False),
    ("Público",        "https://www.publico.es/politica",           "es", False),
    ("elDiario.es",    "https://www.eldiario.es/politica/",         "es", False),
    ("OK Diario",      "https://okdiario.com/espana/",              "es", False),
    ("La Razón",       "https://www.larazon.es/espana/",            "es", False),
    ("El Español",     "https://www.elespanol.com/espana/",         "es", False),
    ("infoLibre",      "https://www.infolibre.es/politica/",        "es", False),
    ("Expansión",      "https://www.expansion.com/economia.html",   "es", False),
    ("El Confidencial","https://www.elconfidencial.com/espana/",    "es", False),
    ("La Sexta",       "https://www.lasexta.com/noticias/nacional/","es", False),
    ("RTVE",           "https://www.rtve.es/noticias/espana/",      "es", False),
    # Fuentes judiciales y de transparencia — clave para cat C y F
    ("Poder Judicial", "https://www.poderjudicial.es/cgpj/es/Poder-Judicial/Noticias-Judiciales/", "es", False),
    ("Transparencia",  "https://www.transparencia.gob.es/transparencia/transparencia_Home/index/Mas-informacion/noticias.html", "es", False),
    ("Civio",          "https://civio.es/noticias/",               "es", False),
    ("El Salto",       "https://www.elsaltodiario.com/politica",    "es", False),
    # Internacionales — contexto europeo
    ("AP News Spain",  "https://apnews.com/hub/spain",              "en", False),
    ("Transparency Intl","https://www.transparency.org/en/news",    "en", False),
    # Fact-checkers — fuente de verdad verificada
    ("Maldita.es",     "https://maldita.es/malditobulo/",           "es", True),
    ("Newtral",        "https://www.newtral.es/zona-verificacion/fact-check/", "es", True),
    ("EFE Verifica",   "https://verifica.efe.com/",                 "es", True),
    ("RTVE Verifica",  "https://www.rtve.es/noticias/verificacion/","es", True),
    ("Snopes",         "https://www.snopes.com/fact-check/",        "en", True),
    ("PolitiFact",     "https://www.politifact.com/factchecks/",    "en", True),
]

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (compatible; ARCAS-Research/2.0; +https://github.com/mgalanme/arcas)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

def scrape_source(name: str, url: str, language: str, is_fc: bool) -> list[dict]:
    try:
        r = requests.get(url, headers=HEADERS_HTTP, timeout=20, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"Scrape failed {name}: {e}")
        return []

    soup  = BeautifulSoup(r.text, "lxml")
    seen  = set()
    items = []

    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(strip=True)
        if len(text) < 25 or text in seen:
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
            "title_es":       "",   # filled later for non-Spanish
            "content_url":    link,
            "pub_date":       date.today().isoformat(),
            "language":       language,
            "jurisdiction":   "ES" if language == "es" else "GL",
            "content_hash":   ch,
            "is_factchecker": is_fc,
        })
        if len(items) >= 30:
            break

    log.info(f"  {name}: {len(items)} articles")
    return items

print("Sources defined:", len(MEDIA_SOURCES))

# COMMAND ----------
# Celda 6: Ingesta BOE

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
            "title_es":       title,  # BOE always in Spanish
            "content_url":    url_html or url_pdf,
            "pub_date":       pub_date.isoformat(),
            "language":       "es",
            "jurisdiction":   "ES",
            "content_hash":   hashlib.sha256(f"{doc_id}|{title}|{pub_date}".encode()).hexdigest(),
            "is_factchecker": False,
        })
    return records

boe_records = []
for days_back in range(3):
    d = date.today() - timedelta(days=days_back)
    recs = fetch_boe(d)
    boe_records.extend(recs)
    log.info(f"BOE {d}: {len(recs)} records")

print(f"BOE total: {len(boe_records)} records")

# COMMAND ----------
# Celda 7: Ingesta medios y fact-checkers

media_records = []
for name, url, lang, is_fc in MEDIA_SOURCES:
    media_records.extend(scrape_source(name, url, lang, is_fc))

print(f"Media total: {len(media_records)} records")

# COMMAND ----------
# Celda 8: Guardar en Delta con deduplicación y traducción

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

all_records = boe_records + media_records
print(f"Total records: {len(all_records)}")

# Deduplicar contra existentes
try:
    existing_hashes = set(
        r.content_hash
        for r in spark.sql(f"SELECT content_hash FROM {TBL_ARTICLES}").collect()
    )
except Exception:
    existing_hashes = set()

new_records = [r for r in all_records if r["content_hash"] not in existing_hashes]
print(f"New records after dedup: {len(new_records)}")

# Traducir títulos en inglés (batch, con pausa para no saturar Groq)
if new_records and GROQ_API_KEY:
    english_records = [r for r in new_records if r["language"] != "es"]
    print(f"Translating {len(english_records)} English titles...")
    for i, r in enumerate(english_records):
        r["title_es"] = translate_to_spanish(r["title"])
        if (i + 1) % 10 == 0:
            time.sleep(3)  # Groq rate limit pause
    # Spanish records: title_es == title
    for r in new_records:
        if r["language"] == "es" and not r["title_es"]:
            r["title_es"] = r["title"]

if new_records:
    df_new = spark.createDataFrame([Row(**r) for r in new_records])
    df_new = df_new.withColumn("ingested_at", current_timestamp())
    df_new.write.format("delta").mode("append").saveAsTable(TBL_ARTICLES)
    print(f"Saved {len(new_records)} new records to {TBL_ARTICLES}")
else:
    print("No new records today.")

print("Ingestion complete")

# COMMAND ----------
# Celda 9: Scoring con keywords mejorados

KW_CAT_A = [
    "contrato", "adjudicaci", "licitaci", "concurso", "subvencion",
    "obra publica", "sobrecoste", "sobreprecio", "comision", "canon",
    "pliego", "concesion", "proveedor", "factura", "malversacion",
]
KW_CAT_B = [
    "patrimonio", "enriquecimiento", "puerta giratoria", "offshore",
    "paraiso fiscal", "testaferro", "blanqueo", "evasion fiscal",
    "cuenta opaca", "bienes ocultos", "fondos", "sociedad pantalla",
]
KW_CAT_C_JUDICIAL = [
    "juez", "tribunal", "sentencia", "fiscal", "sumario",
    "audiencia nacional", "tribunal supremo", "magistrado",
    "instruccion", "juzgado", "imputado", "acusado",
]
KW_CAT_C_BIAS = [
    "archivo", "archiva", "sobresee", "prescripcion", "dilaciones",
    "sin pruebas", "absuelto", "tiempo record", "express",
    "exceso de celo", "guerra sucia", "policia politica",
    "ucо", "udef", "guardia civil", "mossos",
]
KW_CAT_C_POLITICAL = [
    "psoe", "pp", "vox", "podemos", "partido socialista",
    "partido popular", "ciudadanos", "sumar", "junts", "pnv",
    "afiliado", "cargo del partido", "militante",
]
KW_CAT_D = [
    "bulo", "falso", "mentira", "desinformacion", "fake",
    "desmentido", "verificado", "sin evidencia", "manipulado",
    "fuera de contexto", "descontextualizado", "propaganda",
    "viral", "hoax", "rumor",
]
KW_CAT_E = [
    "trama", "blanqueo", "financiacion ilegal", "comisionista",
    "lobbista", "red de influencia", "contactos", "intermediario",
    "donacion irregular", "financiador", "mecenas",
]
KW_CAT_F = [
    "nepotismo", "enchufismo", "cargo de confianza", "incompatibilidad",
    "conflicto de interes", "familiar", "conyuge", "hijo",
    "nombramiento", "tiempo record", "sin meritos", "a dedo",
    "hermanisimo", "cuñado", "chiringuito",
]

THRESHOLD = 0.30

def score(title: str, source_type: str, is_fc: bool) -> dict:
    t = title.lower()

    def kw(lst):
        return min(sum(1 for k in lst if k in t) * 0.15, 0.9)

    def judicial_bias():
        has_j = any(k in t for k in KW_CAT_C_JUDICIAL)
        has_p = any(k in t for k in KW_CAT_C_POLITICAL)
        has_b = any(k in t for k in KW_CAT_C_BIAS)
        if has_j and has_p and has_b: return 0.75
        if has_j and has_p:           return 0.50
        if has_j and has_b:           return 0.50
        if has_j:                     return 0.25
        return 0.0

    gazette_bonus = 0.15 if source_type == "gazette" else 0.0
    fc_bonus      = 0.20 if is_fc else 0.0

    return {
        "cat_a": min(kw(KW_CAT_A) + gazette_bonus, 0.9),
        "cat_b": kw(KW_CAT_B),
        "cat_c": judicial_bias(),
        "cat_d": max(kw(KW_CAT_D), 0.45 if is_fc else 0) + fc_bonus * 0.5,
        "cat_e": kw(KW_CAT_E),
        "cat_f": kw(KW_CAT_F),
    }

def top_category(scores: dict) -> str:
    return max(scores, key=scores.get).replace("cat_", "").upper()

candidates = [r for r in (new_records if new_records else []) if r.get("title")]
scored     = [(r, score(r["title"], r["source_type"], r["is_factchecker"])) for r in candidates]
above      = [(r, s) for r, s in scored if max(s.values()) >= THRESHOLD]

print(f"Candidates: {len(candidates)} | Above threshold: {len(above)}")
for r, s in above[:10]:
    print(f"  [{top_category(s)} {max(s.values()):.2f}] {r['title'][:70]}")

# COMMAND ----------
# Celda 10: Generar hipótesis con Groq en español

import uuid
from datetime import datetime

CAT_DESCRIPTIONS = {
    "A": "fraude en contratación pública o irregularidad contractual",
    "B": "enriquecimiento ilícito o puertas giratorias",
    "C": "anomalía en patrón judicial — posible trato diferencial por afiliación política",
    "D": "desinformación o bulo que beneficia a actores identificables",
    "E": "red de influencia o financiación ilegal",
    "F": "abuso de función pública o nepotismo",
}

alerts_to_save = []
limit = min(len(above), 20)  # max 20 per run to respect Groq limits

for record, scores in above[:limit]:
    cat      = top_category(scores)
    conf     = min(max(scores.values()) + 0.10, 0.99)
    cat_desc = CAT_DESCRIPTIONS.get(cat, "patrón de corrupción")
    title_es = record.get("title_es") or record.get("title", "")

    try:
        hypothesis = groq_invoke(
            f"Eres un analista anticorrupción. Analiza este registro para detectar: {cat_desc}.\n"
            f"Sé factual y conciso. Nunca acuses — señala patrones únicamente.\n\n"
            f"Titular: {title_es}\n"
            f"Fuente: {record['source_name']} ({record['source_type']})\n"
            f"URL: {record['content_url']}\n\n"
            f"Responde en español:\n"
            f"1) Patrón observado\n"
            f"2) Nivel de confianza 0-1\n"
            f"3) Quién podría beneficiarse de este patrón",
            max_tokens=400,
        )
    except Exception as e:
        hypothesis = f"[Patrón detectado] Categoría {cat}: {title_es[:100]}"

    if hypothesis:
        alerts_to_save.append({
            "alert_id":         str(uuid.uuid4()),
            "category":         cat,
            "status":           "pending",
            "confidence_score": round(conf, 3),
            "nl_justification": hypothesis,
            "source_name":      record["source_name"],
            "title":            title_es,
            "content_url":      record["content_url"],
            "created_at":       datetime.utcnow().isoformat(),
        })
        print(f"  [{cat} {conf:.2f}] {title_es[:65]}")

print(f"\nAlerts generated: {len(alerts_to_save)}")

# COMMAND ----------
# Celda 11: Guardar alertas en Delta

if alerts_to_save:
    from pyspark.sql.functions import to_timestamp
    df_alerts = spark.createDataFrame([Row(**a) for a in alerts_to_save])
    df_alerts = df_alerts.withColumn("created_at", to_timestamp("created_at"))
    df_alerts.write.format("delta").mode("append").saveAsTable(TBL_ALERTS)
    print(f"Saved {len(alerts_to_save)} alerts to {TBL_ALERTS}")
else:
    print("No alerts today.")

# Summary
total_articles = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES}").collect()[0]["n"]
total_alerts   = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS}").collect()[0]["n"]
pending_alerts = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS} WHERE status='pending'").collect()[0]["n"]

print(f"\n=== ARCAS Daily Run Summary ===")
print(f"Articles in Delta:  {total_articles}")
print(f"Total alerts:       {total_alerts}")
print(f"Pending HITL:       {pending_alerts}")
print("================================")
