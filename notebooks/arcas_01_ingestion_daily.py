# Databricks notebook source
# ARCAS - 01_ingestion_daily v5
# Cambios v5:
#   - BOE excluido de alertas: solo se usa para contraste, no para analisis de noticias
#   - Threshold diferenciado: medios tienen prioridad sobre fuentes oficiales
#   - Scoring mejorado: bonus fact-checkers, penalizacion fuentes baja credibilidad
#   - Solo candidatos de tipo media/factcheck generan alertas

# COMMAND ----------
# Celda 1: Imports y configuracion

import requests, json, hashlib, re, logging, time
from datetime import date, timedelta
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

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
GROQ_MODEL     = "llama-3.3-70b-versatile"

DB_RAW        = "arcas_raw"
DB_PROCESSED  = "arcas_processed"
TBL_ARTICLES  = f"{DB_RAW}.articles"
TBL_ALERTS    = f"{DB_PROCESSED}.alerts"
TBL_ENTITIES  = f"{DB_PROCESSED}.entities"
TBL_RELATIONS = f"{DB_PROCESSED}.relations"

print(f"GROQ: {bool(GROQ_API_KEY)} | Neo4j: {bool(NEO4J_URI)}")

# COMMAND ----------
# Celda 2: TRUNCAR TABLAS (solo uso manual excepcional - mantener comentado)

# spark.sql(f"TRUNCATE TABLE {TBL_ARTICLES}")
# spark.sql(f"TRUNCATE TABLE {TBL_ALERTS}")
# spark.sql(f"TRUNCATE TABLE {TBL_ENTITIES}")
# spark.sql(f"TRUNCATE TABLE {TBL_RELATIONS}")
# print("Tablas truncadas - recarga completa forzada")

# COMMAND ----------
# Celda 3: Credibilidad de fuentes

SOURCE_CREDIBILITY = {
    "Maldita.es":1.0,"Newtral":1.0,"EFE Verifica":1.0,"RTVE Verifica":1.0,
    "Snopes":1.0,"PolitiFact":1.0,"AP News Spain":0.9,"Transparency Intl":0.9,
    "Civio":0.9,"Poder Judicial":0.9,"Transparencia":0.9,"BOE":0.95,
    "El Pais":0.75,"El Mundo":0.70,"La Vanguardia":0.75,"elDiario.es":0.72,
    "infoLibre":0.70,"El Confidencial":0.72,"RTVE":0.80,"La Sexta":0.70,
    "Expansion":0.72,"El Salto":0.65,"Publico":0.65,
    "ABC":0.60,"La Razon":0.55,"El Espanol":0.58,"OK Diario":0.30,
}

def get_credibility(source): return SOURCE_CREDIBILITY.get(source, 0.60)
print("Credibility OK")

# COMMAND ----------
# Celda 4: Groq helpers

def groq_invoke(prompt, max_tokens=500, temperature=0.1):
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                json={"model":GROQ_MODEL,"messages":[{"role":"user","content":prompt}],
                      "max_tokens":max_tokens,"temperature":temperature},
                timeout=30,
            )
            if r.status_code == 429:
                time.sleep(20*(attempt+1)); continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            log.warning(f"Groq {attempt+1}: {e}"); time.sleep(5)
    return ""

def translate_to_spanish(title):
    markers = ["el ","la ","los ","las ","de ","del ","en ","por ","que ","con "]
    if any(m in title.lower() for m in markers): return title
    r = groq_invoke(f"Traduce al espanol solo el titular:\n{title}", max_tokens=80, temperature=0.0)
    return r if r else title

print("Groq OK")

# COMMAND ----------
# Celda 5: Crear / migrar tablas Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_RAW}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_ARTICLES} (
    source_type STRING, source_name STRING, title STRING, title_es STRING,
    content_url STRING, content_snippet STRING, pub_date STRING, language STRING,
    jurisdiction STRING, content_hash STRING, is_factchecker BOOLEAN, ingested_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TBL_ALERTS} (
    alert_id STRING, category STRING, status STRING, confidence_score DOUBLE,
    nl_justification STRING, deep_analysis STRING, source_name STRING,
    title STRING, content_url STRING, created_at TIMESTAMP
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

for tbl, col, td in [(TBL_ARTICLES,"title_es","STRING"),
                     (TBL_ARTICLES,"content_snippet","STRING"),
                     (TBL_ALERTS,"deep_analysis","STRING")]:
    try: spark.sql(f"ALTER TABLE {tbl} ADD COLUMN {col} {td}")
    except Exception: pass

print("Tables ready")

# COMMAND ----------
# Celda 6: Fuentes

HEADERS_HTTP = {
    "User-Agent":"Mozilla/5.0 (compatible; ARCAS-Research/5.0)",
    "Accept-Language":"es-ES,es;q=0.9,en;q=0.8",
}

MEDIA_SOURCES = [
    ("El Pais","https://elpais.com/espana/","es",False),
    ("El Mundo","https://www.elmundo.es/espana.html","es",False),
    ("ABC","https://www.abc.es/espana/","es",False),
    ("La Vanguardia","https://www.lavanguardia.com/politica","es",False),
    ("Publico","https://www.publico.es/politica","es",False),
    ("elDiario.es","https://www.eldiario.es/politica/","es",False),
    ("OK Diario","https://okdiario.com/espana/","es",False),
    ("La Razon","https://www.larazon.es/espana/","es",False),
    ("El Espanol","https://www.elespanol.com/espana/","es",False),
    ("infoLibre","https://www.infolibre.es/politica/","es",False),
    ("El Confidencial","https://www.elconfidencial.com/espana/","es",False),
    ("La Sexta","https://www.lasexta.com/noticias/nacional/","es",False),
    ("RTVE","https://www.rtve.es/noticias/espana/","es",False),
    ("Expansion","https://www.expansion.com/economia.html","es",False),
    ("Poder Judicial","https://www.poderjudicial.es/cgpj/es/Poder-Judicial/Noticias-Judiciales/","es",False),
    ("Transparencia","https://www.transparencia.gob.es/transparencia/transparencia_Home/index/Mas-informacion/noticias.html","es",False),
    ("Civio","https://civio.es/noticias/","es",False),
    ("El Salto","https://www.elsaltodiario.com/politica","es",False),
    ("AP News Spain","https://apnews.com/hub/spain","en",False),
    ("Transparency Intl","https://www.transparency.org/en/news","en",False),
    ("Maldita.es","https://maldita.es/malditobulo/","es",True),
    ("Newtral","https://www.newtral.es/zona-verificacion/fact-check/","es",True),
    ("EFE Verifica","https://verifica.efe.com/","es",True),
    ("RTVE Verifica","https://www.rtve.es/noticias/verificacion/","es",True),
    ("Snopes","https://www.snopes.com/fact-check/","en",True),
    ("PolitiFact","https://www.politifact.com/factchecks/","en",True),
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
            "source_type":"factcheck" if is_fc else "media","source_name":name,
            "title":text,"title_es":"","content_url":link,"content_snippet":"",
            "pub_date":date.today().isoformat(),"language":language,
            "jurisdiction":"ES" if language=="es" else "GL",
            "content_hash":hashlib.sha256(f"{text}|{name}".encode()).hexdigest(),
            "is_factchecker":is_fc,
        })
        if len(items)>=30: break
    log.info(f"  {name}: {len(items)}")
    return items

print(f"Sources: {len(MEDIA_SOURCES)}")

# COMMAND ----------
# Celda 7: Ingesta BOE (solo para contraste - NO genera alertas)

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
            "is_factchecker":False,
        })
    return records

boe_records = []
for days_back in range(3):
    d = date.today() - timedelta(days=days_back)
    recs = fetch_boe(d)
    boe_records.extend(recs)
    log.info(f"BOE {d}: {len(recs)}")
print(f"BOE (solo contraste): {len(boe_records)}")

# COMMAND ----------
# Celda 8: Ingesta medios

media_records = []
for name, url, lang, is_fc in MEDIA_SOURCES:
    media_records.extend(scrape_source(name, url, lang, is_fc))
print(f"Media: {len(media_records)}")

# COMMAND ----------
# Celda 9: Guardar todos en Delta (BOE + medios para contraste futuro)

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

all_records = boe_records + media_records
try:
    existing = set(r.content_hash for r in spark.sql(f"SELECT content_hash FROM {TBL_ARTICLES}").collect())
except Exception: existing = set()

new_records = [r for r in all_records if r["content_hash"] not in existing]
print(f"New: {len(new_records)}")

if new_records and GROQ_API_KEY:
    eng = [r for r in new_records if r["language"]!="es"]
    for i,r in enumerate(eng):
        r["title_es"] = translate_to_spanish(r["title"])
        if (i+1)%10==0: time.sleep(3)
    for r in new_records:
        if r["language"]=="es" and not r["title_es"]: r["title_es"]=r["title"]

if new_records:
    df = spark.createDataFrame([Row(**r) for r in new_records])
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(TBL_ARTICLES)
    print(f"Saved {len(new_records)} articles")

# COMMAND ----------
# Celda 10: Scoring - SOLO medios y fact-checkers generan alertas
# El BOE se almacena para contraste pero NO entra en el pipeline de alertas

KW_A=["contrato","adjudicaci","licitaci","concurso","subvencion","obra publica",
      "sobrecoste","malversacion","fondos publicos","contrato menor","libre designacion",
      "comision ilegal","soborno","cohecho"]
KW_B=["patrimonio","enriquecimiento","puerta giratoria","offshore","paraiso fiscal",
      "testaferro","blanqueo","evasion fiscal","cuenta opaca","sociedad pantalla"]
KW_CJ=["juez","tribunal","sentencia","fiscal","sumario","audiencia nacional",
       "tribunal supremo","magistrado","instruccion","juzgado","imputado","investigado",
       "acusacion","peinado","garcia-castejón"]
KW_CB=["archivo","archiva","sobresee","prescripcion","dilaciones","sin pruebas",
       "absuelto","tiempo record","express","uco","udef","guardia civil","mossos",
       "nulidad","cautelar","acusacion particular","años de retraso","tardado años",
       "velocidad","rapidez","inmediata","urgente citacion"]
KW_CP=["psoe","pp","vox","podemos","partido socialista","partido popular",
       "ciudadanos","sumar","junts","pnv","afiliado","militante","sanchez","feijoo",
       "abascal","yolanda diaz","begoña","abalos","leire"]
KW_D=["bulo","falso","mentira","desinformacion","fake","desmentido","verificado",
      "sin evidencia","manipulado","fuera de contexto","propaganda","viral","hoax",
      "no es cierto","es falso que","sin pruebas","acusacion sin base",
      "vito quiles","ndongo","bertrand ndongo"]
KW_E=["trama","blanqueo","financiacion ilegal","lobbista","red de influencia",
      "intermediario","donacion irregular","comisionista","kitchen","villarejo"]
KW_F=["nepotismo","enchufismo","cargo de confianza","incompatibilidad",
      "conflicto de interes","nombramiento","a dedo","hermanisimo","cunado",
      "chiringuito","libre designacion","sin meritos","david sanchez"]

THRESHOLD_MEDIA     = 0.28   # umbral para medios generalistas
THRESHOLD_FACTCHECK = 0.20   # umbral mas bajo para fact-checkers (mayor sensibilidad)

def score(title, source_type, is_fc):
    t = title.lower()
    def kw(lst): return min(sum(1 for k in lst if k in t)*0.15, 0.9)
    def jbias():
        hj=any(k in t for k in KW_CJ); hp=any(k in t for k in KW_CP); hb=any(k in t for k in KW_CB)
        if hj and hp and hb: return 0.75
        if hj and (hp or hb): return 0.50
        if hj: return 0.25
        return 0.0
    # Sin bonus para gazette - el BOE no genera alertas de todas formas
    return {
        "cat_a":kw(KW_A),
        "cat_b":kw(KW_B),
        "cat_c":jbias(),
        "cat_d":max(kw(KW_D), 0.50 if is_fc else 0),
        "cat_e":kw(KW_E),
        "cat_f":kw(KW_F),
    }

def top_cat(scores): return max(scores, key=scores.get).replace("cat_","").upper()

# SOLO medios y fact-checkers - el BOE queda excluido del pipeline de alertas
media_candidates = [r for r in (new_records or []) if r.get("title") and r["source_type"] != "gazette"]
scored = [(r, score(r["title"], r["source_type"], r["is_factchecker"])) for r in media_candidates]

above = []
for r, s in scored:
    threshold = THRESHOLD_FACTCHECK if r["is_factchecker"] else THRESHOLD_MEDIA
    if max(s.values()) >= threshold:
        above.append((r, s))

print(f"Media candidates: {len(media_candidates)} | Above threshold: {len(above)}")
for r,s in above[:5]:
    print(f"  [{top_cat(s)} {max(s.values()):.2f}] {r.get('title_es',r['title'])[:65]}")

# COMMAND ----------
# Celda 11: Analisis profundo con calidad probatoria

import uuid
from datetime import datetime

CAT_DESC = {
    "A":"fraude en contratacion publica o malversacion de fondos",
    "B":"enriquecimiento ilicito o puertas giratorias",
    "C":"sesgo judicial o trato procesal diferencial segun afiliacion politica",
    "D":"desinformacion o bulo que beneficia a actores identificables",
    "E":"red de influencia o financiacion ilegal",
    "F":"nepotismo o abuso de funcion publica en nombramientos",
}

DEEP_PROMPT = """Eres un analista experto en corrupcion, derecho procesal y verificacion periodistica.
Analiza con MAXIMO RIGOR y perspectiva POLITICAMENTE NEUTRAL.
Se igualmente exigente con todos los actores politicos sin excepcion.

TITULAR: {title}
FUENTE: {source} (credibilidad documentada: {cred_pct}%)
CATEGORIA: {cat_desc}
CONTENIDO DEL ARTICULO: {snippet}

Responde en espanol con esta estructura:

**PATRON DETECTADO:**
[Describe el patron concreto observado en ESTA noticia especifica. Si no hay evidencia real, indicalo.]

**EVIDENCIAS PRESENTADAS:**
Estado: [Con pruebas materiales / Con testimonios / Solo declaraciones / Sin pruebas — basado en suposiciones]
[Explica que pruebas concretas se mencionan en el articulo, si las hay. Si el articulo se basa en "fuentes", "rumores", declaraciones de partes interesadas o recortes de otros medios sin verificacion independiente, indicalo explicitamente.]

**VELOCIDAD PROCESAL:**
[Solo si hay caso judicial: ¿el proceso es llamativamente rapido o lento? ¿Hay comparativas temporales con casos similares de distinto signo politico? Si no aplica: No procede.]

**SESGO DE LA FUENTE:**
[¿Esta noticia aparece solo en medios de un espectro ideologico o en medios diversos? ¿La fuente tiene historial de publicar informaciones no contrastadas?]

**ACTORES MENCIONADOS:**
[Lista: nombre — rol — partido/organismo]

**NIVEL DE PREOCUPACION:** [Alto / Medio / Bajo / Sin evidencia suficiente]

**COMO VERIFICAR:**
[Que documentos oficiales consultar, que registros publicos revisar para contrastar esta informacion de forma independiente.]"""

ENTITY_PROMPT = """Extrae entidades del texto en JSON valido.
SOLO JSON, sin texto adicional. Formato exacto: {{"personas":[],"organismos":[],"casos":[],"partidos":[]}}

Texto: {text}"""

alerts_to_save   = []
entities_to_save = []
limit = min(len(above), 15)

for record, scores in above[:limit]:
    cat       = top_cat(scores)
    base_conf = max(scores.values())
    title_es  = record.get("title_es") or record.get("title","")
    source    = record["source_name"]
    cred      = get_credibility(source)
    cred_pct  = int(cred*100)

    snippet = record.get("content_snippet","")
    if not snippet and record.get("content_url"):
        snippet = fetch_snippet(record["content_url"]); time.sleep(1)

    analysis = groq_invoke(DEEP_PROMPT.format(
        title=title_es, source=source, cred_pct=cred_pct,
        cat_desc=CAT_DESC.get(cat,"patron de corrupcion"),
        snippet=snippet[:800] if snippet else "(sin contenido — analisis por titular)",
    ), max_tokens=600)
    if not analysis: analysis = f"[Sin analisis] Categoria {cat}: {title_es[:100]}"

    # Ajuste de confianza segun calidad probatoria
    conf = base_conf + 0.10
    al = analysis.lower()
    if "sin pruebas" in al or "sin evidencia" in al or "suposiciones" in al: conf -= 0.20
    elif "solo declaraciones" in al: conf -= 0.10
    elif "con pruebas materiales" in al: conf += 0.15
    conf = round(max(0.10, min(conf * cred, 0.99)), 3)

    alert_id = str(uuid.uuid4())
    alerts_to_save.append({
        "alert_id":alert_id,"category":cat,"status":"pending",
        "confidence_score":conf,"nl_justification":analysis,
        "deep_analysis":analysis,"source_name":source,
        "title":title_es,"content_url":record["content_url"],
        "created_at":datetime.now(datetime.timezone.utc if hasattr(datetime,"timezone") else None).isoformat() if False else datetime.utcnow().isoformat(),
    })
    log.info(f"  [{cat} {conf:.2f} cred={cred_pct}%] {title_es[:60]}")

    ej = groq_invoke(ENTITY_PROMPT.format(text=f"{title_es}. {snippet[:300]}"),
                     max_tokens=200, temperature=0.0)
    try:
        clean = re.sub(r"```json|```","",ej).strip()
        ents = json.loads(clean)
        today_str = date.today().isoformat()
        for etype, names in ents.items():
            for name in (names or []):
                if len(str(name))<3: continue
                eid = hashlib.sha256(f"{etype}|{name}".encode()).hexdigest()[:16]
                entities_to_save.append({
                    "entity_id":eid,"entity_type":etype,"entity_name":str(name),
                    "first_seen":today_str,"last_seen":today_str,"mention_count":1,
                    "sources":source,"alert_ids":alert_id,
                })
    except Exception: pass
    time.sleep(2)

print(f"Alerts: {len(alerts_to_save)} | Entities: {len(entities_to_save)}")

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
# Celda 13: Escribir en Neo4j Aura

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
else:
    print("Neo4j: skipped")

# COMMAND ----------
# Celda 14: Resumen

ta  = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES}").collect()[0]["n"]
tal = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS}").collect()[0]["n"]
pen = spark.sql(f"SELECT count(*) AS n FROM {TBL_ALERTS} WHERE status='pending'").collect()[0]["n"]
te  = spark.sql(f"SELECT count(*) AS n FROM {TBL_ENTITIES}").collect()[0]["n"]
media_arts = spark.sql(f"SELECT count(*) AS n FROM {TBL_ARTICLES} WHERE source_type != 'gazette'").collect()[0]["n"]

print(f"\n=== ARCAS Daily Run v5 ===")
print(f"Articulos medios:       {media_arts}")
print(f"Articulos totales:      {ta} (incl. BOE para contraste)")
print(f"Alertas totales:        {tal}")
print(f"Pendientes revision:    {pen}")
print(f"Entidades en grafo:     {te}")
print("==========================")
