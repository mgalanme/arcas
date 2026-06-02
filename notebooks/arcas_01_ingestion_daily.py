# Databricks notebook source
# ARCAS - 01_ingestion_daily v3
# Cambios v3:
#   - Análisis profundo de corrupción: segunda llamada Groq por artículo candidato
#   - Campo content_snippet extraído del artículo para análisis de contenido real
#   - Score enriquecido con análisis semántico, no solo keywords de titular
#   - Nuevas señales: análisis de contratos BOE por importe y adjudicatario

# COMMAND ----------
# Celda 1: Imports y configuración

import requests, json, hashlib, re, logging, time
from datetime import date, timedelta
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
# Celda 2: Groq helper con retry

def groq_invoke(prompt: str, max_tokens: int = 400, temperature: float = 0.1) -> str:
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
    spanish_markers = ["el ", "la ", "los ", "las ", "de ", "del ", "en ",
                       "por ", "que ", "con ", "una ", "un "]
    if any(m in title.lower() for m in spanish_markers):
        return title
    result = groq_invoke(
        f"Traduce este titular al español. Solo el titular traducido:\n{title}",
        max_tokens=80, temperature=0.0,
    )
    return result if result else title

print("Groq helpers OK")

# COMMAND ----------
# Celda 3: Crear / migrar tablas Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_RAW}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ARTICLES} (
    source_type      STRING,
    source_name      STRING,
    title            STRING,
    title_es         STRING,
    content_url      STRING,
    content_snippet  STRING,
    pub_date         STRING,
    language         STRING,
    jurisdiction     STRING,
    content_hash     STRING,
    is_factchecker   BOOLEAN,
    ingested_at      TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ALERTS} (
    alert_id           STRING,
    category           STRING,
    status             STRING,
    confidence_score   DOUBLE,
    nl_justification   STRING,
    deep_analysis      STRING,
    source_name        STRING,
    title              STRING,
    content_url        STRING,
    created_at         TIMESTAMP
) USING DELTA
""")

# Idempotent column migrations
for col, typedef in [
    ("title_es",        "STRING"),
    ("content_snippet", "STRING"),
    ("deep_analysis",   "STRING"),
]:
    for tbl in [TBL_ARTICLES]:
        try:
            spark.sql(f"ALTER TABLE {tbl} ADD COLUMN {col} {typedef}")
            print(f"Added {col} to {tbl}")
        except Exception:
            pass
for col, typedef in [("deep_analysis", "STRING")]:
    try:
        spark.sql(f"ALTER TABLE {TBL_ALERTS} ADD COLUMN {col} {typedef}")
        print(f"Added {col} to {TBL_ALERTS}")
    except Exception:
        pass

print("Tables ready")

# COMMAND ----------
# Celda 4: Fuentes

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (compatible; ARCAS-Research/3.0; +https://github.com/mgalanme/arcas)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

MEDIA_SOURCES = [
    # Prensa generalista
    ("El País",          "https://elpais.com/espana/",                "es", False),
    ("El Mundo",         "https://www.elmundo.es/espana.html",         "es", False),
    ("ABC",              "https://www.abc.es/espana/",                 "es", False),
    ("La Vanguardia",    "https://www.lavanguardia.com/politica",      "es", False),
    ("Público",          "https://www.publico.es/politica",            "es", False),
    ("elDiario.es",      "https://www.eldiario.es/politica/",          "es", False),
    ("OK Diario",        "https://okdiario.com/espana/",               "es", False),
    ("La Razón",         "https://www.larazon.es/espana/",             "es", False),
    ("El Español",       "https://www.elespanol.com/espana/",          "es", False),
    ("infoLibre",        "https://www.infolibre.es/politica/",         "es", False),
    ("El Confidencial",  "https://www.elconfidencial.com/espana/",     "es", False),
    ("La Sexta",         "https://www.lasexta.com/noticias/nacional/", "es", False),
    ("RTVE",             "https://www.rtve.es/noticias/espana/",       "es", False),
    ("Expansión",        "https://www.expansion.com/economia.html",    "es", False),
    # Fuentes judiciales y de transparencia
    ("Poder Judicial",   "https://www.poderjudicial.es/cgpj/es/Poder-Judicial/Noticias-Judiciales/", "es", False),
    ("Transparencia",    "https://www.transparencia.gob.es/transparencia/transparencia_Home/index/Mas-informacion/noticias.html", "es", False),
    ("Civio",            "https://civio.es/noticias/",                 "es", False),
    ("El Salto",         "https://www.elsaltodiario.com/politica",     "es", False),
    # Internacionales
    ("AP News Spain",    "https://apnews.com/hub/spain",               "en", False),
    ("Transparency Intl","https://www.transparency.org/en/news",       "en", False),
    # Fact-checkers
    ("Maldita.es",       "https://maldita.es/malditobulo/",            "es", True),
    ("Newtral",          "https://www.newtral.es/zona-verificacion/fact-check/", "es", True),
    ("EFE Verifica",     "https://verifica.efe.com/",                  "es", True),
    ("RTVE Verifica",    "https://www.rtve.es/noticias/verificacion/", "es", True),
    ("Snopes",           "https://www.snopes.com/fact-check/",         "en", True),
    ("PolitiFact",       "https://www.politifact.com/factchecks/",     "en", True),
]

def fetch_article_snippet(url: str, max_chars: int = 800) -> str:
    """Fetch the first meaningful paragraphs of an article for deep analysis."""
    try:
        r = requests.get(url, headers=HEADERS_HTTP, timeout=15, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        # Remove nav, footer, scripts
        for tag in soup(["nav", "footer", "script", "style", "aside", "header"]):
            tag.decompose()
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 60]
        snippet = " ".join(paragraphs[:5])
        return snippet[:max_chars]
    except Exception:
        return ""

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
            link = "/".join(url.split("/")[:3]) + link
        elif not link.startswith("http"):
            link = url
        ch = hashlib.sha256(f"{text}|{name}".encode()).hexdigest()
        items.append({
            "source_type":    "factcheck" if is_fc else "media",
            "source_name":    name,
            "title":          text,
            "title_es":       "",
            "content_url":    link,
            "content_snippet": "",
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

print(f"Sources defined: {len(MEDIA_SOURCES)}")

# COMMAND ----------
# Celda 5: Ingesta BOE

BOE_API = "https://www.boe.es/datosabiertos/api/boe/sumario"

# Keywords BOE que indican contratos y nombramientos relevantes
BOE_CORRUPTION_PATTERNS = re.compile(
    r"(adjudicaci|licitaci|contrato|concurso|subvenci|obra|nombramiento|"
    r"resoluci.*cargo|cese.*cargo|libre designaci|confianza|delegaci)",
    re.IGNORECASE
)

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
        # Extract department/organism for context
        dept = (item.findtext("departamento") or item.findtext("origen") or "").strip()
        url_html = (item.findtext("url_html") or "").strip()
        url_pdf  = (item.findtext("url_pdf") or "").strip()
        # For BOE, snippet is title + department
        snippet = f"{dept}: {title}" if dept else title
        records.append({
            "source_type":     "gazette",
            "source_name":     "BOE",
            "title":           title,
            "title_es":        title,
            "content_url":     url_html or url_pdf,
            "content_snippet": snippet,
            "pub_date":        pub_date.isoformat(),
            "language":        "es",
            "jurisdiction":    "ES",
            "content_hash":    hashlib.sha256(f"{doc_id}|{title}|{pub_date}".encode()).hexdigest(),
            "is_factchecker":  False,
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
# Celda 6: Ingesta medios

media_records = []
for name, url, lang, is_fc in MEDIA_SOURCES:
    media_records.extend(scrape_source(name, url, lang, is_fc))
print(f"Media total: {len(media_records)} records")

# COMMAND ----------
# Celda 7: Guardar en Delta con dedup y traducción

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

all_records = boe_records + media_records
print(f"Total records: {len(all_records)}")

try:
    existing_hashes = set(
        r.content_hash
        for r in spark.sql(f"SELECT content_hash FROM {TBL_ARTICLES}").collect()
    )
except Exception:
    existing_hashes = set()

new_records = [r for r in all_records if r["content_hash"] not in existing_hashes]
print(f"New records after dedup: {len(new_records)}")

if new_records and GROQ_API_KEY:
    english_records = [r for r in new_records if r["language"] != "es"]
    print(f"Translating {len(english_records)} English titles...")
    for i, r in enumerate(english_records):
        r["title_es"] = translate_to_spanish(r["title"])
        if (i + 1) % 10 == 0:
            time.sleep(3)
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

# COMMAND ----------
# Celda 8: Scoring con keywords

KW_CAT_A = [
    "contrato", "adjudicaci", "licitaci", "concurso", "subvencion",
    "obra publica", "sobrecoste", "sobreprecio", "comision", "canon",
    "pliego", "concesion", "proveedor", "malversacion", "fondos publicos",
    "contrato menor", "fraccionamiento", "libre designacion",
]
KW_CAT_B = [
    "patrimonio", "enriquecimiento", "puerta giratoria", "offshore",
    "paraiso fiscal", "testaferro", "blanqueo", "evasion fiscal",
    "cuenta opaca", "bienes ocultos", "sociedad pantalla", "comisionista",
]
KW_CAT_C_JUDICIAL = [
    "juez", "tribunal", "sentencia", "fiscal", "sumario",
    "audiencia nacional", "tribunal supremo", "magistrado",
    "instruccion", "juzgado", "imputado", "acusado", "investigado",
]
KW_CAT_C_BIAS = [
    "archivo", "archiva", "sobresee", "prescripcion", "dilaciones",
    "sin pruebas", "absuelto", "tiempo record", "express",
    "guerra sucia", "policia politica", "uco", "udef",
    "guardia civil", "mossos", "errores procesales",
]
KW_CAT_C_POLITICAL = [
    "psoe", "pp", "vox", "podemos", "partido socialista",
    "partido popular", "ciudadanos", "sumar", "junts", "pnv",
    "afiliado", "militante", "cargo del partido",
]
KW_CAT_D = [
    "bulo", "falso", "mentira", "desinformacion", "fake",
    "desmentido", "verificado", "sin evidencia", "manipulado",
    "fuera de contexto", "propaganda", "viral", "hoax", "rumor",
    "no es cierto", "es falso que",
]
KW_CAT_E = [
    "trama", "blanqueo", "financiacion ilegal", "lobbista",
    "red de influencia", "intermediario", "donacion irregular",
    "financiador", "comisionista", "fondos opacos",
]
KW_CAT_F = [
    "nepotismo", "enchufismo", "cargo de confianza", "incompatibilidad",
    "conflicto de interes", "nombramiento", "a dedo",
    "hermanisimo", "cunado", "chiringuito", "libre designacion",
    "sin meritos", "familiar directo",
]

THRESHOLD = 0.28

def score(title: str, source_type: str, is_fc: bool) -> dict:
    t = title.lower()
    def kw(lst): return min(sum(1 for k in lst if k in t) * 0.15, 0.9)
    def judicial_bias():
        hj = any(k in t for k in KW_CAT_C_JUDICIAL)
        hp = any(k in t for k in KW_CAT_C_POLITICAL)
        hb = any(k in t for k in KW_CAT_C_BIAS)
        if hj and hp and hb: return 0.75
        if hj and (hp or hb): return 0.50
        if hj: return 0.25
        return 0.0
    gazette_bonus = 0.15 if source_type == "gazette" else 0.0
    return {
        "cat_a": min(kw(KW_CAT_A) + gazette_bonus, 0.9),
        "cat_b": kw(KW_CAT_B),
        "cat_c": judicial_bias(),
        "cat_d": max(kw(KW_CAT_D), 0.45 if is_fc else 0),
        "cat_e": kw(KW_CAT_E),
        "cat_f": kw(KW_CAT_F),
    }

def top_category(scores: dict) -> str:
    return max(scores, key=scores.get).replace("cat_", "").upper()

candidates = [r for r in (new_records if new_records else []) if r.get("title")]
scored     = [(r, score(r["title"], r["source_type"], r["is_factchecker"])) for r in candidates]
above      = [(r, s) for r, s in scored if max(s.values()) >= THRESHOLD]
print(f"Candidates: {len(candidates)} | Above threshold ({THRESHOLD}): {len(above)}")

# COMMAND ----------
# Celda 9: Análisis profundo de corrupción con Groq
# Para los candidatos que superan el umbral, se obtiene el snippet del artículo
# y se hace una segunda llamada a Groq que analiza el contenido real,
# no solo el titular. Esto detecta corrupción que no aparece en keywords.

import uuid
from datetime import datetime

CAT_DESCRIPTIONS = {
    "A": "fraude en contratación pública, adjudicación irregular o malversación de fondos públicos",
    "B": "enriquecimiento ilícito, puertas giratorias o evasión fiscal por parte de cargos públicos",
    "C": "sesgo judicial o trato procesal diferencial según afiliación política del investigado",
    "D": "desinformación, bulo o manipulación informativa que beneficia a actores políticos identificables",
    "E": "red de influencia, tráfico de influencias o financiación ilegal de partidos",
    "F": "nepotismo, enchufismo o abuso de función pública en nombramientos",
}

DEEP_ANALYSIS_PROMPT = """Eres un analista experto en corrupción pública, periodismo de investigación y transparencia institucional.

Analiza el siguiente contenido periodístico y determina si hay indicios de corrupción o irregularidad pública.

TITULAR: {title}
FUENTE: {source} ({source_type})
CATEGORÍA DETECTADA: {cat_desc}
CONTENIDO: {snippet}

Responde en español con esta estructura exacta:

**PATRÓN DETECTADO:**
[Describe el patrón de irregularidad observado. Si no hay evidencia real de corrupción, indícalo claramente.]

**ACTORES IMPLICADOS:**
[Personas, organismos o entidades mencionadas que podrían estar involucradas.]

**NIVEL DE PREOCUPACIÓN:** [Alto / Medio / Bajo / Sin evidencia]
[Justifica en una frase.]

**QUIÉN SE BENEFICIA:**
[Quién podría beneficiarse si el patrón descrito fuera real.]

**ACCIÓN RECOMENDADA:**
[Qué debería hacer un ciudadano o periodista para verificar esto: qué documentos pedir, qué registros consultar.]"""

alerts_to_save = []
limit = min(len(above), 15)

for record, scores in above[:limit]:
    cat      = top_category(scores)
    conf     = min(max(scores.values()) + 0.10, 0.99)
    cat_desc = CAT_DESCRIPTIONS.get(cat, "patrón de corrupción")
    title_es = record.get("title_es") or record.get("title", "")

    # Fetch article snippet for deep analysis
    snippet = record.get("content_snippet", "")
    if not snippet and record.get("content_url"):
        log.info(f"  Fetching snippet for: {title_es[:50]}")
        snippet = fetch_article_snippet(record["content_url"])
        time.sleep(1)  # polite crawling

    # Deep analysis via Groq
    prompt = DEEP_ANALYSIS_PROMPT.format(
        title      = title_es,
        source     = record["source_name"],
        source_type= record["source_type"],
        cat_desc   = cat_desc,
        snippet    = snippet[:600] if snippet else "(contenido no disponible — análisis basado en titular)",
    )
    deep_analysis = groq_invoke(prompt, max_tokens=500)

    if not deep_analysis:
        deep_analysis = f"[Análisis no disponible] Categoría {cat}: {title_es[:100]}"

    # Confidence boost if deep analysis finds real evidence
    if "sin evidencia" in deep_analysis.lower() or "no hay indicio" in deep_analysis.lower():
        conf = max(conf - 0.15, 0.20)  # penalise if no real evidence

    alerts_to_save.append({
        "alert_id":          str(uuid.uuid4()),
        "category":          cat,
        "status":            "pending",
        "confidence_score":  round(conf, 3),
        "nl_justification":  deep_analysis,
        "deep_analysis":     deep_analysis,
        "source_name":       record["source_name"],
        "title":             title_es,
        "content_url":       record["content_url"],
        "created_at":        datetime.utcnow().isoformat(),
    })
    log.info(f"  [{cat} {conf:.2f}] {title_es[:65]}")
    time.sleep(2)  # Groq rate limit

print(f"\nAlerts generated: {len(alerts_to_save)}")

# COMMAND ----------
# Celda 10: Guardar alertas

if alerts_to_save:
    from pyspark.sql.functions import to_timestamp
    df_alerts = spark.createDataFrame([Row(**a) for a in alerts_to_save])
    df_alerts = df_alerts.withColumn("created_at", to_timestamp("created_at"))
    df_alerts.write.format("delta").mode("append").saveAsTable(TBL_ALERTS)
    print(f"Saved {len(alerts_to_save)} alerts to {TBL_ALERTS}")
else:
    print("No alerts today.")

total_articles = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES}").collect()[0]["n"]
total_alerts   = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS}").collect()[0]["n"]
pending_alerts = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS} WHERE status='pending'").collect()[0]["n"]

print(f"\n=== ARCAS Daily Run Summary ===")
print(f"Artículos procesados: {total_articles}")
print(f"Alertas totales:      {total_alerts}")
print(f"Pendientes revisión:  {pending_alerts}")
print("================================")
