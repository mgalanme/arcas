# Databricks notebook source
# ARCAS - 01_ingestion_daily v7
# Cambios v7 (FIX RENDIMIENTO - foco celda 9 y pipeline completo):
#   - Deduplicación SIN .collect() de TODO el histórico: Spark left_anti join sobre content_hash.
#     Solo se hace .collect() de los registros NUEVOS del día (normalmente <100).
#   - Traducción (title_es) y clasificación de topic en BATCH (1-4 llamadas Groq en vez de N).
#     Prompt único con múltiples artículos + parseo de JSON array.
#   - Clasificador HEURÍSTICO por keywords (reutiliza espíritu de las KW del scoring) antes de LLM.
#     Reduce llamadas LLM drásticamente para la mayoría de artículos en español.
#   - Modelo rápido (llama-3.1-8b-instant) para translate + classify (el 70b se reserva para deep analysis).
#   - Paralelismo controlado con ThreadPoolExecutor (respeta ~30 RPM de Groq).
#   - Celda 8 (scrape) también paralelizada (I/O bound) → ingesta completa mucho más rápida.
#   - Celda 11 (deep analysis) paralelizada (hasta 15 items) para reducir tiempo de alertas.
#   - Añadidos timing por fase + logs de reducción de llamadas.
#   - groq_invoke mejorado: soporta model override + mejor manejo de rate limits.
#   - Mantiene 100% la misma lógica, mismas tablas Delta, mismas alertas, entidades, Neo4j y salida que v6.
#   - Total: de O(N) llamadas secuenciales + full table collect → O(1) llamadas batch + trabajo distribuido Spark.

# COMMAND ----------
# Celda 1: Imports y configuracion

import requests, json, hashlib, re, logging, time
from datetime import date, timedelta
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def get_param(key, fallback=""):
    try:
        return dbutils.secrets.get(scope="arcas", key=key.lower())
    except Exception:
        pass
    try:
        return dbutils.widgets.get(key)
    except Exception:
        return fallback

GROQ_API_KEY   = get_param("GROQ_API_KEY")
NEO4J_URI      = get_param("NEO4J_URI")
NEO4J_USER     = get_param("NEO4J_USERNAME")
NEO4J_PASSWORD = get_param("NEO4J_PASSWORD")
GROQ_MODEL      = "llama-3.3-70b-versatile"     # Para deep analysis (calidad)
GROQ_FAST_MODEL = "llama-3.1-8b-instant"        # Para translate + topic classify (velocidad + más RPM)

DB_RAW        = "arcas_raw"
DB_PROCESSED  = "arcas_processed"
TBL_ARTICLES  = f"{DB_RAW}.articles"
TBL_ALERTS    = f"{DB_PROCESSED}.alerts"
TBL_ENTITIES  = f"{DB_PROCESSED}.entities"
TBL_RELATIONS = f"{DB_PROCESSED}.relations"

VALID_TOPICS = ["POLITICA","JUDICIAL","ECONOMIA","SALUD","DESINFORMACION","PSEUDOCIENCIA","OTRO"]

# Keywords mínimas para clasificación heurística de topic (temprano en el script)
TOPIC_KEYWORDS = {
    "JUDICIAL": [
        "juez", "tribunal", "sentencia", "fiscal", "sumario", "audiencia nacional",
        "tribunal supremo", "magistrado", "instruccion", "juzgado", "imputado", "investigado",
        "peinado", "garcia-castejon", "acusacion", "fiscalia", "delito", "penal"
    ],
    "POLITICA": [
        "psoe", "pp", "vox", "podemos", "partido socialista", "partido popular",
        "ciudadanos", "sumar", "junts", "pnv", "sanchez", "feijoo", "abascal",
        "yolanda diaz", "begoña", "abalos", "gobierno", "congreso", "senado", "elecciones",
        "coalicion", "presupuesto", "ministro", "vicepresident"
    ],
    "ECONOMIA": [
        "economia", "bolsa", "ibex", "inflacion", "paro", "empleo", "pib", "presupuestos",
        "impuestos", "deuda", "deficit", "crecimiento", "empresa", "adjudicacion", "licitacion",
        "contrato publico", "subvencion"
    ],
    "SALUD": [
        "salud", "vacuna", "covid", "hospital", "medico", "enfermedad", "cancer",
        "pandemia", "sanitario", "oms", "medicamento", "terapia"
    ],
    "DESINFORMACION": [
        "bulo", "falso", "mentira", "desinformacion", "fake", "desmentido", "verificado",
        "sin evidencia", "manipulado", "fuera de contexto", "propaganda", "viral", "hoax",
        "no es cierto", "es falso que", "sin pruebas", "acusacion sin base",
        "vito quiles", "ndongo", "bertrand ndongo"
    ],
    "PSEUDOCIENCIA": [
        "homeopatia", "homeopatico", "pseudociencia", "pseudoterapia", "curandero",
        "milagro", "cura milagrosa", "medicina alternativa", "sin evidencia cientifica",
        "no avalado", "conspiracion", "chemtrails", "antivacunas", "ivermectina",
        "terraplanismo", "esoterico", "astrologico", "cristaloterapia", "sanacion energetica",
        "medicina cuantica", "bioresonancia", "flores de bach"
    ],
}

print(f"GROQ: {bool(GROQ_API_KEY)} | Neo4j: {bool(NEO4J_URI)} | Fast model: {GROQ_FAST_MODEL}")

# COMMAND ----------
# Celda 2: TRUNCAR (solo uso manual excepcional)

# spark.sql(f"TRUNCATE TABLE {TBL_ARTICLES}")
# spark.sql(f"TRUNCATE TABLE {TBL_ALERTS}")
# spark.sql(f"TRUNCATE TABLE {TBL_ENTITIES}")
# spark.sql(f"TRUNCATE TABLE {TBL_RELATIONS}")
# print("Truncado completo")

# COMMAND ----------
# Celda 3: Credibilidad de fuentes

SOURCE_CREDIBILITY = {
    "Maldita.es":1.0,"Maldita Ciencia":1.0,"Newtral":1.0,"EFE Verifica":1.0,
    "RTVE Verifica":1.0,"Snopes":1.0,"PolitiFact":1.0,
    "AP News Spain":0.9,"Transparency Intl":0.9,"Civio":0.9,
    "Poder Judicial":0.9,"Transparencia":0.9,"BOE":0.95,
    "El Pais":0.75,"El Pais Salud":0.75,"El Mundo":0.70,"El Mundo Salud":0.70,
    "La Vanguardia":0.75,"elDiario.es":0.72,"infoLibre":0.70,
    "El Confidencial":0.72,"El Confidencial Salud":0.70,
    "RTVE":0.80,"La Sexta":0.70,"Expansion":0.72,"El Salto":0.65,"Publico":0.65,
    "ABC":0.60,"La Razon":0.55,"El Espanol":0.58,
    "OK Diario":0.30,"20minutos":0.45,"El Espanol Ciencia":0.50,
}

def get_credibility(source): return SOURCE_CREDIBILITY.get(source, 0.60)
print("Credibility OK")

# COMMAND ----------
# Celda 4: Groq helpers (mejorados v7)

def groq_invoke(prompt, max_tokens=500, temperature=0.1, model=None):
    """Llamada a Groq con reintentos y soporte de modelo override (fast vs quality)."""
    model = model or GROQ_MODEL
    for attempt in range(4):
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type":"application/json"},
                json={"model": model,
                      "messages":[{"role":"user","content":prompt}],
                      "max_tokens":max_tokens,"temperature":temperature},
                timeout=45,
            )
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                log.warning(f"Groq 429 - waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            log.warning(f"Groq {attempt+1}/{4} ({model}): {e}")
            time.sleep(4 + attempt * 2)
    return ""

def translate_to_spanish(title):
    """Individual (fallback)."""
    markers = ["el ","la ","los ","las ","de ","del ","en ","por ","que ","con "]
    if any(m in title.lower() for m in markers): return title
    r = groq_invoke(f"Traduce al espanol solo el titular:\n{title}",
                    max_tokens=80, temperature=0.0, model=GROQ_FAST_MODEL)
    return r if r else title

def classify_topic(title, snippet=""):
    """Individual (fallback)."""
    prompt = f"""Clasifica este articulo periodistico en UNA de estas categorias:
POLITICA, JUDICIAL, ECONOMIA, SALUD, DESINFORMACION, PSEUDOCIENCIA, OTRO

Responde SOLO con la palabra de la categoria, sin explicaciones.

Titular: {title}
Contenido: {snippet[:200] if snippet else ""}"""
    result = groq_invoke(prompt, max_tokens=10, temperature=0.0, model=GROQ_FAST_MODEL)
    result = result.strip().upper().split()[0] if result else "OTRO"
    return result if result in VALID_TOPICS else "OTRO"

def heuristic_classify(title, snippet="", source_name="", is_fc=False):
    """Clasificación rápida por keywords (sin LLM)."""
    text = f"{title or ''} {snippet or ''}".lower()
    best_topic = "OTRO"
    best_score = 0
    for topic, kws in TOPIC_KEYWORDS.items():
        sc = sum(1 for kw in kws if kw in text)
        if sc > best_score:
            best_score = sc
            best_topic = topic
    # Bias para fact-checkers de desinfo/pseudo
    if is_fc and best_topic not in ("DESINFORMACION", "PSEUDOCIENCIA"):
        if any(kw in text for kw in TOPIC_KEYWORDS["DESINFORMACION"][:6]):
            best_topic = "DESINFORMACION"
        elif any(kw in text for kw in TOPIC_KEYWORDS["PSEUDOCIENCIA"][:6]):
            best_topic = "PSEUDOCIENCIA"
    return best_topic if best_score >= 1 else None

def translate_titles_batch(records):
    """Traduce múltiples títulos en 1-2 llamadas Groq (JSON batch)."""
    to_translate = [r for r in records if r.get("language") != "es" and not r.get("title_es")]
    if not to_translate:
        return 0
    if len(to_translate) <= 2:
        for r in to_translate:
            r["title_es"] = translate_to_spanish(r["title"])
        return len(to_translate)

    lines = [f'{i}: {r.get("title","")}' for i, r in enumerate(to_translate)]
    prompt = f"""Traduce al español SOLO los siguientes titulares periodísticos.
Responde EXCLUSIVAMENTE un JSON array válido SIN explicaciones:
[{{"idx": 0, "title_es": "..."}}, ...]

Titulares:
""" + "\n".join(lines)

    resp = groq_invoke(prompt, max_tokens=600, temperature=0.0, model=GROQ_FAST_MODEL)
    try:
        clean = re.sub(r"```json|```", "", resp or "").strip()
        arr = json.loads(clean)
        mapping = {}
        for x in (arr or []):
            try:
                mapping[int(x.get("idx", -1))] = (x.get("title_es") or "").strip()
            except Exception:
                pass
        for i, r in enumerate(to_translate):
            r["title_es"] = mapping.get(i) or r.get("title", "")
        log.info(f"  Batch translate OK: {len(to_translate)} titles in 1 call")
        return 1  # 1 LLM call
    except Exception as e:
        log.warning(f"Batch translate parse failed, fallback individual: {e}")
        for r in to_translate:
            r["title_es"] = translate_to_spanish(r["title"])
        return len(to_translate)  # worst case

def classify_topics_batch(records, use_heuristic=True):
    """Clasifica múltiples artículos con heurística + 1 batch LLM si hace falta."""
    if not records:
        return 0

    to_classify = []
    heuristic_hits = 0
    for r in records:
        if r.get("topic") and r["topic"] != "":
            continue
        h = None
        if use_heuristic:
            h = heuristic_classify(
                r.get("title_es") or r.get("title", ""),
                r.get("content_snippet", ""),
                r.get("source_name", ""),
                r.get("is_factchecker", False)
            )
            if h:
                r["topic"] = h
                heuristic_hits += 1
                continue
        to_classify.append(r)

    if not to_classify:
        log.info(f"  Heuristic classified {heuristic_hits} (no LLM needed)")
        return 0

    # Batch LLM para los que quedan
    lines = []
    for i, r in enumerate(to_classify):
        ttl = (r.get("title_es") or r.get("title", ""))[:180]
        sn  = (r.get("content_snippet") or "")[:120]
        lines.append(f'{i}: Título: {ttl} | Snippet: {sn}')

    prompt = f"""Clasifica CADA artículo periodístico en EXACTAMENTE UNA de estas categorías:
{', '.join(VALID_TOPICS)}

Responde SOLO un JSON array válido (sin texto extra):
[{{"idx": 0, "topic": "POLITICA"}}, {{"idx": 1, "topic": "JUDICIAL"}}, ...]

Artículos a clasificar:
""" + "\n".join(lines)

    resp = groq_invoke(prompt, max_tokens=400, temperature=0.0, model=GROQ_FAST_MODEL)
    llm_calls = 1
    try:
        clean = re.sub(r"```json|```", "", resp or "").strip()
        arr = json.loads(clean)
        idx_map = {}
        for x in (arr or []):
            try:
                idx = int(x.get("idx", -1))
                t = str(x.get("topic", "OTRO")).upper().strip()
                idx_map[idx] = t if t in VALID_TOPICS else "OTRO"
            except Exception:
                pass
        for i, r in enumerate(to_classify):
            r["topic"] = idx_map.get(i, "OTRO")
        log.info(f"  Batch classify: {heuristic_hits} heuristic + {len(to_classify)} via 1 LLM call")
    except Exception as e:
        log.warning(f"Batch classify parse failed ({e}), fallback to individual calls")
        for r in to_classify:
            r["topic"] = classify_topic(r.get("title_es") or r.get("title", ""), r.get("content_snippet", ""))
        llm_calls = len(to_classify)  # worst
    return llm_calls

print("Groq helpers v7 (batch + heuristic + fast model) OK")

# COMMAND ----------
# Celda 5: Crear / migrar tablas Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_RAW}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_ARTICLES} (
    source_type STRING, source_name STRING, title STRING, title_es STRING,
    content_url STRING, content_snippet STRING, pub_date STRING, language STRING,
    jurisdiction STRING, content_hash STRING, is_factchecker BOOLEAN,
    topic STRING, ingested_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_ALERTS} (
    alert_id STRING, category STRING, topic STRING, status STRING,
    confidence_score DOUBLE, nl_justification STRING, deep_analysis STRING,
    source_name STRING, title STRING, content_url STRING, created_at TIMESTAMP
) USING DELTA""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_ENTITIES} (
    entity_id STRING, entity_type STRING, entity_name STRING,
    first_seen DATE, last_seen DATE, mention_count INT,
    sources STRING, alert_ids STRING
) USING DELTA""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_RELATIONS} (
    relation_id STRING, entity_from STRING, relation_type STRING,
    entity_to STRING, evidence STRING, first_seen DATE,
    last_seen DATE, mention_count INT
) USING DELTA""")

for tbl, col, td in [
    (TBL_ARTICLES,"title_es","STRING"),
    (TBL_ARTICLES,"content_snippet","STRING"),
    (TBL_ARTICLES,"topic","STRING"),
    (TBL_ALERTS,"deep_analysis","STRING"),
    (TBL_ALERTS,"topic","STRING"),
]:
    try: spark.sql(f"ALTER TABLE {tbl} ADD COLUMN {col} {td}")
    except Exception: pass

print("Tables ready")

# COMMAND ----------
# Celda 6: Fuentes — medios + fact-checkers + pseudociencias/salud

HEADERS_HTTP = {
    "User-Agent":"Mozilla/5.0 (compatible; ARCAS-Research/7.0)",
    "Accept-Language":"es-ES,es;q=0.9,en;q=0.8",
}

MEDIA_SOURCES = [
    # Prensa generalista
    ("El Pais",          "https://elpais.com/espana/",                "es", False),
    ("El Mundo",         "https://www.elmundo.es/espana.html",         "es", False),
    ("ABC",              "https://www.abc.es/espana/",                 "es", False),
    ("La Vanguardia",    "https://www.lavanguardia.com/politica",      "es", False),
    ("Publico",          "https://www.publico.es/politica",            "es", False),
    ("elDiario.es",      "https://www.eldiario.es/politica/",          "es", False),
    ("OK Diario",        "https://okdiario.com/espana/",               "es", False),
    ("La Razon",         "https://www.larazon.es/espana/",             "es", False),
    ("El Espanol",       "https://www.elespanol.com/espana/",          "es", False),
    ("infoLibre",        "https://www.infolibre.es/politica/",         "es", False),
    ("El Confidencial",  "https://www.elconfidencial.com/espana/",     "es", False),
    ("La Sexta",         "https://www.lasexta.com/noticias/nacional/", "es", False),
    ("RTVE",             "https://www.rtve.es/noticias/espana/",       "es", False),
    ("Expansion",        "https://www.expansion.com/economia.html",    "es", False),
    # Fuentes judiciales y transparencia
    ("Poder Judicial",   "https://www.poderjudicial.es/cgpj/es/Poder-Judicial/Noticias-Judiciales/", "es", False),
    ("Transparencia",    "https://www.transparencia.gob.es/transparencia/transparencia_Home/index/Mas-informacion/noticias.html", "es", False),
    ("Civio",            "https://civio.es/noticias/",                 "es", False),
    ("El Salto",         "https://www.elsaltodiario.com/politica",     "es", False),
    # Salud y pseudociencias
    ("El Pais Salud",    "https://elpais.com/salud-y-bienestar/",      "es", False),
    ("El Mundo Salud",   "https://www.elmundo.es/ciencia-y-salud/salud.html", "es", False),
    ("El Confidencial Salud","https://www.elconfidencial.com/bienestar/", "es", False),
    ("20minutos",        "https://www.20minutos.es/ciencia/",          "es", False),
    # Internacionales
    ("AP News Spain",    "https://apnews.com/hub/spain",               "en", False),
    ("Transparency Intl","https://www.transparency.org/en/news",       "en", False),
    # Fact-checkers generalistas
    ("Maldita.es",       "https://maldita.es/malditobulo/",            "es", True),
    ("Newtral",          "https://www.newtral.es/zona-verificacion/fact-check/", "es", True),
    ("EFE Verifica",     "https://verifica.efe.com/",                  "es", True),
    ("RTVE Verifica",    "https://www.rtve.es/noticias/verificacion/", "es", True),
    ("Snopes",           "https://www.snopes.com/fact-check/",         "en", True),
    ("PolitiFact",       "https://www.politifact.com/factchecks/",     "en", True),
    # Fact-checkers ciencia y salud
    ("Maldita Ciencia",  "https://maldita.es/malditaciencia/",         "es", True),
]

def fetch_snippet(url, max_chars=1000):
    try:
        r = requests.get(url, headers=HEADERS_HTTP, timeout=15, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["nav","footer","script","style","aside","header"]): tag.decompose()
        paras = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True))>60]
        return " ".join(paras[:6])[:max_chars]
    except Exception: return ""

def scrape_source(name, url, language, is_fc):
    try:
        r = requests.get(url, headers=HEADERS_HTTP, timeout=20, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"Scrape failed {name}: {e}"); return []
    soup = BeautifulSoup(r.text, "lxml")
    seen, items = set(), []
    for tag in soup.find_all(["h1","h2","h3"]):
        text = tag.get_text(strip=True)
        if len(text)<25 or text in seen: continue
        seen.add(text)
        a = tag.find("a", href=True)
        link = a["href"] if a else url
        if link.startswith("/"): link = "/".join(url.split("/")[:3]) + link
        elif not link.startswith("http"): link = url
        items.append({
            "source_type":"factcheck" if is_fc else "media",
            "source_name":name,"title":text,"title_es":"",
            "content_url":link,"content_snippet":"",
            "pub_date":date.today().isoformat(),"language":language,
            "jurisdiction":"ES" if language=="es" else "GL",
            "content_hash":hashlib.sha256(f"{text}|{name}".encode()).hexdigest(),
            "is_factchecker":is_fc,"topic":"",
        })
        if len(items)>=30: break
    log.info(f"  {name}: {len(items)}")
    return items

print(f"Sources: {len(MEDIA_SOURCES)}")

# COMMAND ----------
# Celda 7: Ingesta BOE (solo contraste)

def fetch_boe(pub_date):
    url = f"https://www.boe.es/datosabiertos/api/boe/sumario/{pub_date.strftime('%Y%m%d')}"
    try:
        r = requests.get(url, headers={"Accept":"application/xml"}, timeout=30)
        if r.status_code==404: return []
        r.raise_for_status()
        root = ET.fromstring(r.text)
    except Exception as e:
        log.warning(f"BOE {pub_date}: {e}"); return []
    if root.findtext(".//status/code")!="200": return []
    records = []
    for item in root.findall(".//item"):
        doc_id=(item.findtext("identificador") or "").strip()
        title=(item.findtext("titulo") or "").strip()
        if not doc_id or not title: continue
        dept=(item.findtext("departamento") or item.findtext("origen") or "").strip()
        url_h=(item.findtext("url_html") or "").strip()
        url_p=(item.findtext("url_pdf") or "").strip()
        records.append({
            "source_type":"gazette","source_name":"BOE","title":title,"title_es":title,
            "content_url":url_h or url_p,
            "content_snippet":f"{dept}: {title}" if dept else title,
            "pub_date":pub_date.isoformat(),"language":"es","jurisdiction":"ES",
            "content_hash":hashlib.sha256(f"{doc_id}|{title}|{pub_date}".encode()).hexdigest(),
            "is_factchecker":False,"topic":"OFICIAL",
        })
    return records

boe_records = []
for days_back in range(3):
    d = date.today() - timedelta(days=days_back)
    recs = fetch_boe(d)
    boe_records.extend(recs)
    log.info(f"BOE {d}: {len(recs)}")
print(f"BOE (contraste): {len(boe_records)}")

# COMMAND ----------
# Celda 8: Ingesta medios (PARALELIZADA v7 - mucho más rápida)

def scrape_all_parallel(sources, max_workers=7):
    """Ejecuta scrape_source en paralelo (I/O-bound). Respeta el mismo contrato."""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_src = {
            executor.submit(scrape_source, name, url, lang, is_fc): name
            for name, url, lang, is_fc in sources
        }
        for future in as_completed(future_to_src):
            name = future_to_src[future]
            try:
                items = future.result()
                results.extend(items)
            except Exception as exc:
                log.warning(f"Scrape {name} generated exception: {exc}")
    return results

t_scrape = time.time()
media_records = scrape_all_parallel(MEDIA_SOURCES)
print(f"Media (parallel): {len(media_records)} in {time.time()-t_scrape:.1f}s")

# COMMAND ----------
# Celda 9: Guardar en Delta con dedup, traduccion y clasificacion de topic (REFACTORIZADA v7)

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

t9 = time.time()
all_records = boe_records + media_records

new_records = []
if all_records:
    # Creamos DF de candidatos (driver solo maneja los del día actual)
    cand_rows = [Row(**r) for r in all_records]
    cand_df = spark.createDataFrame(cand_rows)

    # Anti-join distribuido en Spark: NO hacemos collect de millones de hashes
    existing_hashes = spark.table(TBL_ARTICLES).select("content_hash").distinct()
    new_df = cand_df.join(existing_hashes, on="content_hash", how="left_anti")

    # Solo recolectamos los NUEVOS (casi siempre decenas, no cientos de miles)
    new_records = [r.asDict() for r in new_df.collect()]

print(f"New: {len(new_records)} (dedup via Spark left_anti join - sin full collect histórico)")

llm_calls_estimate = 0
if new_records and GROQ_API_KEY:
    # 1. Traducciones en batch (títulos en inglés)
    t_tr = time.time()
    eng = [r for r in new_records if r.get("language") != "es"]
    calls_tr = translate_titles_batch(eng)
    llm_calls_estimate += calls_tr
    for r in new_records:
        if r.get("language") == "es" and not r.get("title_es"):
            r["title_es"] = r.get("title", "")
    print(f"  Translations: {len(eng)} items, ~{calls_tr} LLM call(s) in {time.time()-t_tr:.1f}s")

    # 2. Clasificación de topic: heurística + batch LLM
    t_cl = time.time()
    media_new = [r for r in new_records if r.get("source_type") != "gazette"]
    print(f"Classifying topics for {len(media_new)} media articles (heuristic-first + batched LLM)...")
    calls_cl = classify_topics_batch(media_new, use_heuristic=True)
    llm_calls_estimate += calls_cl
    # BOE ya trae topic="OFICIAL"
    print(f"  Topics done in {time.time()-t_cl:.1f}s (heuristic + ~{calls_cl} LLM call(s))")

if new_records:
    # Aseguramos campos obligatorios por si algún record viene incompleto
    for r in new_records:
        r.setdefault("title_es", r.get("title", ""))
        r.setdefault("topic", "OTRO" if r.get("source_type") != "gazette" else "OFICIAL")
        r.setdefault("content_snippet", "")

    df_new = spark.createDataFrame([Row(**r) for r in new_records])
    df_new = df_new.withColumn("ingested_at", current_timestamp())

    # Usamos MERGE para escritura idempotente (mejor práctica Delta + dedup extra)
    df_new.createOrReplaceTempView("new_enriched")
    spark.sql(f"""
        MERGE INTO {TBL_ARTICLES} t
        USING new_enriched n
        ON t.content_hash = n.content_hash
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {len(new_records)} new articles into Delta (idempotent)")

print(f"Celda 9 total: {time.time()-t9:.1f}s | LLM calls estimadas para enriquecimiento: ~{llm_calls_estimate} (vs N en v6)")

# COMMAND ----------
# Celda 10: Scoring — SOLO medios/fact-checkers, ampliado con pseudociencias

KW_A=["contrato","adjudicaci","licitaci","concurso","subvencion","obra publica",
      "sobrecoste","malversacion","fondos publicos","contrato menor","libre designacion",
      "comision ilegal","soborno","cohecho"]
KW_B=["patrimonio","enriquecimiento","puerta giratoria","offshore","paraiso fiscal",
      "testaferro","blanqueo","evasion fiscal","cuenta opaca","sociedad pantalla"]
KW_CJ=["juez","tribunal","sentencia","fiscal","sumario","audiencia nacional",
       "tribunal supremo","magistrado","instruccion","juzgado","imputado","investigado",
       "peinado","garcia-castejón","acusacion"]
KW_CB=["archivo","archiva","sobresee","prescripcion","dilaciones","sin pruebas",
       "absuelto","tiempo record","express","uco","udef","guardia civil","mossos",
       "nulidad","cautelar","acusacion particular","tardado años","velocidad procesal"]
KW_CP=["psoe","pp","vox","podemos","partido socialista","partido popular",
       "ciudadanos","sumar","junts","pnv","afiliado","militante","sanchez","feijoo",
       "abascal","yolanda diaz","begoña","abalos","leire"]
KW_D=["bulo","falso","mentira","desinformacion","fake","desmentido","verificado",
      "sin evidencia","manipulado","fuera de contexto","propaganda","viral","hoax",
      "no es cierto","es falso que","sin pruebas","acusacion sin base",
      "vito quiles","ndongo","bertrand ndongo"]
KW_PSEUDO=["homeopatia","homeopatico","pseudociencia","pseudoterapia","curandero",
           "milagro","cura milagrosa","medicina alternativa","sin evidencia cientifica",
           "no avalado","conspiracion","chemtrails","antivacunas","ivermectina",
           "terraplanismo","esoterico","astrologico","cristaloterapia","sanacion energetica",
           "medicina cuantica","bioresonancia","flores de bach"]
KW_E=["trama","blanqueo","financiacion ilegal","lobbista","red de influencia",
      "intermediario","donacion irregular","comisionista","kitchen","villarejo"]
KW_F=["nepotismo","enchufismo","cargo de confianza","incompatibilidad",
      "conflicto de interes","nombramiento","a dedo","hermanisimo","cunado",
      "chiringuito","libre designacion","sin meritos","david sanchez"]

THRESHOLD_MEDIA     = 0.28
THRESHOLD_FACTCHECK = 0.20

def score(title, source_type, is_fc):
    t = title.lower()
    def kw(lst): return min(sum(1 for k in lst if k in t)*0.15, 0.9)
    def jbias():
        hj=any(k in t for k in KW_CJ); hp=any(k in t for k in KW_CP)
        hb=any(k in t for k in KW_CB)
        if hj and hp and hb: return 0.75
        if hj and (hp or hb): return 0.50
        if hj: return 0.25
        return 0.0
    pseudo_score = max(kw(KW_PSEUDO), 0.55 if is_fc and any(k in t for k in KW_PSEUDO) else 0)
    return {
        "cat_a":kw(KW_A),
        "cat_b":kw(KW_B),
        "cat_c":jbias(),
        "cat_d":max(kw(KW_D), pseudo_score, 0.50 if is_fc else 0),
        "cat_e":kw(KW_E),
        "cat_f":kw(KW_F),
    }

def top_cat(s): return max(s, key=s.get).replace("cat_","").upper()

media_candidates = [r for r in (new_records or [])
                    if r.get("title") and r["source_type"]!="gazette"]
scored = [(r, score(r["title"], r["source_type"], r["is_factchecker"]))
          for r in media_candidates]
above = []
for r,s in scored:
    thr = THRESHOLD_FACTCHECK if r["is_factchecker"] else THRESHOLD_MEDIA
    if max(s.values())>=thr: above.append((r,s))

print(f"Media candidates: {len(media_candidates)} | Above threshold: {len(above)}")

# COMMAND ----------
# Celda 11: Analisis profundo (PARALELIZADO v7)

import uuid
from datetime import datetime

CAT_DESC = {
    "A":"fraude en contratacion publica o malversacion de fondos",
    "B":"enriquecimiento ilicito o puertas giratorias",
    "C":"sesgo judicial o trato procesal diferencial segun afiliacion politica",
    "D":"desinformacion, bulo o pseudociencia que perjudica a ciudadanos",
    "E":"red de influencia o financiacion ilegal",
    "F":"nepotismo o abuso de funcion publica",
}

DEEP_PROMPT = """Eres un analista experto en corrupcion, derecho procesal, verificacion periodistica y pseudociencias.
Analiza con MAXIMO RIGOR y perspectiva POLITICAMENTE NEUTRAL.

TITULAR: {title}
FUENTE: {source} (credibilidad: {cred_pct}%)
CATEGORIA: {cat_desc}
CONTENIDO: {snippet}

Responde en espanol:

**PATRON DETECTADO:**
[Describe el patron concreto de ESTA noticia. Si no hay evidencia, indicalo.]

**EVIDENCIAS PRESENTADAS:**
Estado: [Con pruebas materiales / Con testimonios / Solo declaraciones / Sin pruebas — basado en suposiciones]
[Que pruebas concretas se mencionan. Si se basa en rumores, declaraciones de partes o recortes sin verificacion independiente, indicalo sin ambiguedad.]

**IMPACTO EN CIUDADANOS:**
[Como afecta esto directamente al bolsillo, la salud o las decisiones de los ciudadanos. Si es pseudociencia, que riesgos reales puede causar creer en esto.]

**VELOCIDAD PROCESAL:**
[Solo si hay caso judicial: es llamativamente rapido o lento vs casos similares de distinto signo? Si no aplica: No procede.]

**SESGO DE LA FUENTE:**
[Aparece en medios de un solo espectro ideologico o en fuentes diversas? La fuente tiene historial de bulos?]

**ACTORES MENCIONADOS:**
[nombre — rol — partido/organismo]

**NIVEL DE PREOCUPACION:** [Alto / Medio / Bajo / Sin evidencia suficiente]

**COMO VERIFICAR:**
[Documentos oficiales, registros publicos o fuentes cientificas para contrastar de forma independiente.]"""

ENTITY_PROMPT = """Extrae entidades del texto en JSON valido.
SOLO JSON. Formato: {{"personas":[],"organismos":[],"casos":[],"partidos":[]}}
Texto: {text}"""

def _process_one_for_alert(record, scores):
    """Procesa un candidato: deep analysis + entities. Devuelve (alert_dict, list_entities)."""
    cat       = top_cat(scores)
    base_conf = max(scores.values())
    title_es  = record.get("title_es") or record.get("title","")
    source    = record["source_name"]
    cred      = get_credibility(source)
    cred_pct  = int(cred*100)
    topic     = record.get("topic","OTRO")

    snippet = record.get("content_snippet","")
    if not snippet and record.get("content_url"):
        snippet = fetch_snippet(record["content_url"])

    analysis = groq_invoke(DEEP_PROMPT.format(
        title=title_es, source=source, cred_pct=cred_pct,
        cat_desc=CAT_DESC.get(cat,"patron de corrupcion"),
        snippet=snippet[:800] if snippet else "(sin contenido)",
    ), max_tokens=650)
    if not analysis: analysis = f"[Sin analisis] {cat}: {title_es[:100]}"

    conf = base_conf + 0.10
    al = analysis.lower()
    if "sin pruebas" in al or "sin evidencia" in al or "suposiciones" in al: conf -= 0.20
    elif "solo declaraciones" in al: conf -= 0.10
    elif "con pruebas materiales" in al: conf += 0.15
    conf = round(max(0.10, min(conf*cred, 0.99)), 3)

    alert_id = str(uuid.uuid4())
    alert = {
        "alert_id":alert_id,"category":cat,"topic":topic,"status":"pending",
        "confidence_score":conf,"nl_justification":analysis,"deep_analysis":analysis,
        "source_name":source,"title":title_es,"content_url":record["content_url"],
        "created_at":datetime.utcnow().isoformat(),
    }
    log.info(f"  [{cat}/{topic} {conf:.2f} {cred_pct}%] {title_es[:55]}")

    entities = []
    ej = groq_invoke(ENTITY_PROMPT.format(text=f"{title_es}. {snippet[:300]}"),
                     max_tokens=200, temperature=0.0, model=GROQ_FAST_MODEL)
    try:
        clean = re.sub(r"```json|```","",ej).strip()
        ents = json.loads(clean)
        today_str = date.today().isoformat()
        for etype, names in ents.items():
            for name in (names or []):
                if len(str(name))<3: continue
                eid = hashlib.sha256(f"{etype}|{name}".encode()).hexdigest()[:16]
                entities.append({
                    "entity_id":eid,"entity_type":etype,"entity_name":str(name),
                    "first_seen":today_str,"last_seen":today_str,"mention_count":1,
                    "sources":source,"alert_ids":alert_id,
                })
    except Exception:
        pass

    return alert, entities

alerts_to_save   = []
entities_to_save = []
limit = min(len(above), 15)

t11 = time.time()
if above and limit > 0:
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(_process_one_for_alert, record, scores): (record, scores)
            for record, scores in above[:limit]
        }
        for fut in as_completed(futures):
            try:
                al, ents = fut.result()
                alerts_to_save.append(al)
                entities_to_save.extend(ents)
            except Exception as e:
                log.warning(f"Alert processing error: {e}")

print(f"Alerts: {len(alerts_to_save)} | Entities: {len(entities_to_save)} (parallel deep analysis in {time.time()-t11:.1f}s)")

# COMMAND ----------
# Celda 12: Guardar alertas y entidades en Delta

if alerts_to_save:
    from pyspark.sql.functions import to_timestamp
    df_a = spark.createDataFrame([Row(**a) for a in alerts_to_save])
    df_a = df_a.withColumn("created_at", to_timestamp("created_at"))
    df_a.write.format("delta").mode("append").saveAsTable(TBL_ALERTS)
    print(f"Saved {len(alerts_to_save)} alerts")

if entities_to_save:
    from pyspark.sql.functions import to_date, col
    df_e = spark.createDataFrame([Row(**e) for e in entities_to_save])
    df_e = df_e.withColumn("first_seen", to_date("first_seen")) \
               .withColumn("last_seen",  to_date("last_seen")) \
               .withColumn("mention_count", col("mention_count").cast("int"))
    df_e.write.format("delta").mode("append").saveAsTable(TBL_ENTITIES)
    print(f"Saved {len(entities_to_save)} entities")

# COMMAND ----------
# Celda 13: Neo4j

if NEO4J_URI and entities_to_save:
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        def upsert(tx, e):
            tx.run("""
                MERGE (n:Entity {entity_id:$eid})
                ON CREATE SET n.name=$name,n.type=$etype,n.first_seen=$fs,n.mention_count=1
                ON MATCH SET  n.last_seen=$ls,n.mention_count=n.mention_count+1
            """, eid=e["entity_id"],name=e["entity_name"],etype=e["entity_type"],
                fs=e["first_seen"],ls=e["last_seen"])
        def link(tx, e, src, aid):
            tx.run("""
                MERGE (s:Source {name:$src})
                MERGE (a:Alert {alert_id:$aid})
                MERGE (n:Entity {entity_id:$eid})
                MERGE (n)-[:MENTIONED_IN]->(a)
                MERGE (a)-[:FROM_SOURCE]->(s)
            """, src=src, aid=aid, eid=e["entity_id"])
        with driver.session() as sess:
            for e in entities_to_save:
                sess.execute_write(upsert, e)
                sess.execute_write(link, e, e["sources"], e["alert_ids"])
        driver.close()
        print(f"Neo4j: {len(entities_to_save)} entities")
    except Exception as ex:
        log.warning(f"Neo4j failed: {ex}")

# COMMAND ----------
# Celda 14: Resumen

ta   = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES}").collect()[0]["n"]
tm   = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES} WHERE source_type!='gazette'").collect()[0]["n"]
tal  = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS}").collect()[0]["n"]
pen  = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS} WHERE status='pending'").collect()[0]["n"]
te   = spark.sql(f"SELECT count(*) AS n FROM {TBL_ENTITIES}").collect()[0]["n"]

print(f"\n=== ARCAS Daily Run v7 (rendimiento optimizado) ===")
print(f"Articulos medios:    {tm}")
print(f"Articulos totales:   {ta} (incl. BOE contraste)")
print(f"Alertas totales:     {tal}")
print(f"Pendientes:          {pen}")
print(f"Entidades grafo:     {te}")
print("====================================================")
print("Notas v7: dedup distribuido + batch LLM (translate/classify) + heuristic + parallel scraping & analysis.")