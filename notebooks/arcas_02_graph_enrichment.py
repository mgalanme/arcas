# Databricks notebook source
# ARCAS - 02_graph_enrichment
# Nivel 2: enriquecimiento semanal del grafo de entidades
#
# Qué hace este notebook:
#   1. Consolida menciones duplicadas de la misma entidad en una sola fila
#   2. Calcula métricas temporales por entidad: dias activo, frecuencia, tendencia
#   3. Detecta patrones comparativos de velocidad procesal entre casos
#   4. Escribe entidades consolidadas en Neo4j con métricas enriquecidas
#   5. Genera alertas de nivel 2 si detecta anomalías temporales
#
# Ejecutar: manualmente o como Job semanal independiente

# COMMAND ----------

# Celda 1: Imports y configuracion

import requests, json, hashlib, re, logging, time
from datetime import date, timedelta, datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

DB_PROCESSED   = "arcas_processed"
TBL_ALERTS     = f"{DB_PROCESSED}.alerts"
TBL_ENTITIES   = f"{DB_PROCESSED}.entities"
TBL_ENTITY_TIMELINE = f"{DB_PROCESSED}.entity_timeline"

print(f"GROQ: {bool(GROQ_API_KEY)} | Neo4j: {bool(NEO4J_URI)}")

# COMMAND ----------

# Celda 2: Crear tabla entity_timeline si no existe
# Esta tabla es el corazon del Nivel 2:
# acumula la historia completa de cada entidad a lo largo del tiempo

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_PROCESSED}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ENTITY_TIMELINE} (
    entity_id        STRING,
    entity_name      STRING,
    entity_type      STRING,
    first_seen       DATE,
    last_seen        DATE,
    total_mentions   INT,
    active_days      INT,
    mentions_per_week DOUBLE,
    trend            STRING,
    peak_week        STRING,
    associated_cats  STRING,
    associated_topics STRING,
    sources_list     STRING,
    alert_count      INT,
    velocity_score   DOUBLE,
    anomaly_flag     BOOLEAN,
    anomaly_reason   STRING,
    updated_at       TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("entity_timeline table ready")

# COMMAND ----------

# Celda 3: Consolidar entidades duplicadas desde TBL_ENTITIES

df_ent = spark.sql(f"""
    SELECT
        entity_name,
        entity_type,
        min(first_seen)                  AS first_seen,
        max(last_seen)                   AS last_seen,
        sum(mention_count)               AS total_mentions,
        collect_set(sources)             AS sources_set,
        collect_set(alert_ids)           AS alert_ids_set,
        count(*)                         AS row_count
    FROM {TBL_ENTITIES}
    WHERE entity_name IS NOT NULL
      AND length(trim(entity_name)) > 2
    GROUP BY entity_name, entity_type
""")

print(f"Unique entities: {df_ent.count()}")
df_ent.show(10, truncate=40)

# COMMAND ----------

# Celda 4: Calcular metricas temporales

df_metrics = df_ent.withColumn(
    "active_days",
    F.datediff(F.col("last_seen"), F.col("first_seen")) + 1
).withColumn(
    "weeks_active",
    F.greatest(F.lit(1), (F.datediff(F.col("last_seen"), F.col("first_seen")) / 7).cast("int"))
).withColumn(
    "mentions_per_week",
    F.round(F.col("total_mentions") / F.col("weeks_active"), 2)
).withColumn(
    "sources_list",
    F.array_join(F.col("sources_set"), ", ")
).withColumn(
    "alert_count",
    F.size(F.col("alert_ids_set"))
)

# Tendencia: comparar menciones recientes vs historico
# Si last_seen es reciente (< 7 dias) y tiene muchas menciones → CRECIENTE
today = date.today()
df_metrics = df_metrics.withColumn(
    "days_since_last",
    F.datediff(F.lit(today), F.col("last_seen"))
).withColumn(
    "trend",
    F.when(
        (F.col("days_since_last") <= 7) & (F.col("mentions_per_week") >= 3), "CRECIENTE"
    ).when(
        (F.col("days_since_last") <= 14) & (F.col("mentions_per_week") >= 2), "ACTIVO"
    ).when(
        F.col("days_since_last") > 30, "INACTIVO"
    ).otherwise("ESTABLE")
)

print("Metrics calculated")
df_metrics.select("entity_name","entity_type","active_days",
                  "total_mentions","mentions_per_week","trend").show(15, truncate=30)

# COMMAND ----------

# Celda 5: Cruzar entidades con alertas para obtener categorias y topics

df_alerts_summary = spark.sql(f"""
    SELECT
        alert_id,
        category,
        topic,
        confidence_score,
        created_at
    FROM {TBL_ALERTS}
""")

# Join entidades con alertas via alert_ids_set
# Explode alert_ids para hacer el join
df_ent_exploded = df_metrics.select(
    "entity_name", "entity_type",
    F.explode("alert_ids_set").alias("alert_id_raw")
).withColumn(
    "alert_id", F.trim(F.col("alert_id_raw"))
)

df_ent_cats = df_ent_exploded.join(
    df_alerts_summary, df_ent_exploded.alert_id == df_alerts_summary.alert_id, "left"
).groupBy("entity_name", "entity_type").agg(
    F.collect_set("category").alias("cats"),
    F.collect_set("topic").alias("topics"),
    F.round(F.avg("confidence_score"), 3).alias("avg_confidence")
).withColumn(
    "associated_cats",   F.array_join(F.col("cats"), ",")
).withColumn(
    "associated_topics", F.array_join(F.col("topics"), ",")
)

df_final = df_metrics.join(
    df_ent_cats.select("entity_name","entity_type",
                       "associated_cats","associated_topics","avg_confidence"),
    on=["entity_name","entity_type"], how="left"
)

print(f"Entities with category/topic enrichment: {df_final.count()}")

# COMMAND ----------

# Celda 6: Detectar anomalias de velocidad procesal
# Comparar entidades judiciales entre si:
# Si una persona/caso politico tiene active_days muy corto vs otros → sospechoso
# Si tiene active_days muy largo → posible dilacion

df_judicial = df_final.filter(
    (F.col("associated_cats").contains("C")) |
    (F.col("associated_topics").contains("JUDICIAL"))
)

judicial_count = df_judicial.count()
print(f"Judicial entities: {judicial_count}")

if judicial_count > 1:
    stats = df_judicial.select(
        F.mean("active_days").alias("mean_days"),
        F.stddev("active_days").alias("std_days"),
        F.mean("mentions_per_week").alias("mean_freq"),
        F.stddev("mentions_per_week").alias("std_freq"),
    ).collect()[0]

    mean_days = float(stats["mean_days"] or 0)
    std_days  = float(stats["std_days"]  or 1)
    mean_freq = float(stats["mean_freq"] or 0)
    std_freq  = float(stats["std_freq"]  or 1)

    log.info(f"Judicial baseline: {mean_days:.0f}±{std_days:.0f} days, "
             f"{mean_freq:.1f}±{std_freq:.1f} mentions/week")

    # Anomaly: active_days > mean + 1.5*std → dilacion sospechosa
    # Anomaly: active_days < mean - 1.5*std AND mentions_per_week high → velocidad sospechosa
    df_final = df_final.withColumn(
        "velocity_score",
        F.when(
            F.col("associated_cats").contains("C") | F.col("associated_topics").contains("JUDICIAL"),
            F.round((F.col("active_days") - mean_days) / F.lit(max(std_days, 1)), 2)
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "anomaly_flag",
        F.when(
            (F.col("velocity_score") > 1.5),
            F.lit(True)
        ).when(
            (F.col("velocity_score") < -1.5) & (F.col("mentions_per_week") > mean_freq + std_freq),
            F.lit(True)
        ).otherwise(F.lit(False))
    ).withColumn(
        "anomaly_reason",
        F.when(
            F.col("velocity_score") > 1.5,
            F.lit("Duración procesal significativamente superior a la media")
        ).when(
            (F.col("velocity_score") < -1.5) & (F.col("mentions_per_week") > mean_freq + std_freq),
            F.lit("Proceso resuelto con rapidez inusual bajo alta cobertura mediática")
        ).otherwise(F.lit(""))
    )
else:
    df_final = df_final.withColumn("velocity_score",  F.lit(0.0)) \
                       .withColumn("anomaly_flag",     F.lit(False)) \
                       .withColumn("anomaly_reason",   F.lit(""))

anomalies = df_final.filter(F.col("anomaly_flag") == True).count()
print(f"Anomalies detected: {anomalies}")

# COMMAND ----------

# Celda 7: Guardar entity_timeline en Delta

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

df_to_save = df_final.select(
    F.md5(F.concat_ws("|", F.col("entity_name"), F.col("entity_type"))).alias("entity_id"),
    F.col("entity_name"),
    F.col("entity_type"),
    F.col("first_seen"),
    F.col("last_seen"),
    F.col("total_mentions").cast(IntegerType()),
    F.col("active_days").cast(IntegerType()),
    F.col("mentions_per_week").cast(DoubleType()),
    F.col("trend"),
    F.lit(None).cast("string").alias("peak_week"),
    F.coalesce(F.col("associated_cats"),   F.lit("")).alias("associated_cats"),
    F.coalesce(F.col("associated_topics"), F.lit("")).alias("associated_topics"),
    F.col("sources_list"),
    F.col("alert_count").cast(IntegerType()),
    F.coalesce(F.col("velocity_score"), F.lit(0.0)).cast(DoubleType()).alias("velocity_score"),
    F.coalesce(F.col("anomaly_flag"),   F.lit(False)).cast(BooleanType()).alias("anomaly_flag"),
    F.coalesce(F.col("anomaly_reason"), F.lit("")).alias("anomaly_reason"),
    current_timestamp().alias("updated_at"),
)

# Overwrite: esta tabla siempre refleja el estado consolidado actual
df_to_save.write.format("delta").mode("overwrite").saveAsTable(TBL_ENTITY_TIMELINE)
total = df_to_save.count()
print(f"Saved {total} entities to {TBL_ENTITY_TIMELINE}")

# COMMAND ----------

# Celda 8: Escribir entidades enriquecidas en Neo4j

if NEO4J_URI and total > 0:
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        def upsert_enriched(tx, e):
            tx.run("""
                MERGE (n:Entity {entity_id: $eid})
                SET n.name             = $name,
                    n.type             = $etype,
                    n.first_seen       = $first_seen,
                    n.last_seen        = $last_seen,
                    n.total_mentions   = $mentions,
                    n.active_days      = $active_days,
                    n.mentions_per_week= $mpw,
                    n.trend            = $trend,
                    n.velocity_score   = $vs,
                    n.anomaly_flag     = $anomaly,
                    n.anomaly_reason   = $reason,
                    n.associated_cats  = $cats,
                    n.updated_at       = $updated
            """, eid=e["entity_id"], name=e["entity_name"], etype=e["entity_type"],
                first_seen=str(e["first_seen"]), last_seen=str(e["last_seen"]),
                mentions=int(e["total_mentions"] or 0),
                active_days=int(e["active_days"] or 0),
                mpw=float(e["mentions_per_week"] or 0),
                trend=e["trend"] or "ESTABLE",
                vs=float(e["velocity_score"] or 0),
                anomaly=bool(e["anomaly_flag"]),
                reason=e["anomaly_reason"] or "",
                cats=e["associated_cats"] or "",
                updated=datetime.utcnow().isoformat())

        def link_anomaly(tx, e):
            """Si hay anomalia, crear nodo Pattern y relacionarlo con la entidad."""
            tx.run("""
                MERGE (p:Pattern {type: 'VELOCITY_ANOMALY', entity_id: $eid})
                SET p.description = $reason,
                    p.velocity_score = $vs,
                    p.detected_at = $now
                MERGE (n:Entity {entity_id: $eid})
                MERGE (n)-[:HAS_PATTERN]->(p)
            """, eid=e["entity_id"], reason=e["anomaly_reason"] or "",
                vs=float(e["velocity_score"] or 0),
                now=datetime.utcnow().isoformat())

        rows = df_to_save.collect()
        with driver.session() as sess:
            for row in rows:
                r = row.asDict()
                sess.execute_write(upsert_enriched, r)
                if r.get("anomaly_flag"):
                    sess.execute_write(link_anomaly, r)
                    log.info(f"  ANOMALY: {r['entity_name']} — {r['anomaly_reason']}")

        driver.close()
        print(f"Neo4j: upserted {len(rows)} entities")
    except Exception as ex:
        log.warning(f"Neo4j failed: {ex}")
else:
    print("Neo4j: skipped")

# COMMAND ----------

# Celda 9: Generar alertas de nivel 2 para anomalias detectadas

import uuid

df_anomalies = df_to_save.filter(F.col("anomaly_flag") == True).collect()
alerts_l2 = []

for row in df_anomalies:
    r = row.asDict()
    name    = r["entity_name"]
    reason  = r["anomaly_reason"]
    vs      = float(r["velocity_score"] or 0)
    days    = int(r["active_days"] or 0)
    mpw     = float(r["mentions_per_week"] or 0)
    cats    = r["associated_cats"] or ""
    sources = r["sources_list"] or ""

    # Groq: generar justificacion narrativa de la anomalia
    if GROQ_API_KEY:
        prompt = f"""Eres un analista de patrones judiciales. Analiza esta anomalía detectada:

Entidad: {name}
Tipo: {r['entity_type']}
Días activo en seguimiento: {days}
Menciones por semana: {mpw:.1f}
Razón de la anomalía: {reason}
Fuentes que lo mencionan: {sources[:200]}

Escribe en español un análisis breve (3-4 frases) de por qué esta anomalía es relevante
para la ciudadanía y qué patrón podría indicar. Sé factual y neutral. No acuses directamente."""
        analysis = ""
        try:
            r2 = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": GROQ_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 250, "temperature": 0.1},
                timeout=20,
            )
            r2.raise_for_status()
            analysis = r2.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            analysis = f"Anomalía detectada: {reason}"
        time.sleep(3)
    else:
        analysis = f"Anomalía detectada: {reason}"

    alerts_l2.append({
        "alert_id":         str(uuid.uuid4()),
        "category":         "C",
        "topic":            "JUDICIAL",
        "status":           "pending",
        "confidence_score": round(min(0.5 + abs(vs) * 0.15, 0.95), 3),
        "nl_justification": analysis,
        "deep_analysis":    analysis,
        "source_name":      "ARCAS Nivel 2 — Análisis temporal",
        "title":            f"[PATRÓN TEMPORAL] {name}: {reason[:80]}",
        "content_url":      "",
        "created_at":       datetime.utcnow().isoformat(),
    })
    log.info(f"  L2 alert: {name} (vs={vs:.2f})")

if alerts_l2:
    from pyspark.sql import Row
    from pyspark.sql.functions import to_timestamp
    df_l2 = spark.createDataFrame([Row(**a) for a in alerts_l2])
    df_l2 = df_l2.withColumn("created_at", to_timestamp("created_at"))
    df_l2.write.format("delta").mode("append").saveAsTable(TBL_ALERTS)
    print(f"Generated {len(alerts_l2)} Level-2 alerts")
else:
    print("No Level-2 anomalies this run")

# COMMAND ----------

# Celda 10: Resumen

te = spark.sql(f"SELECT count(*) AS n FROM {TBL_ENTITY_TIMELINE}").collect()[0]["n"]
ta = spark.sql(f"SELECT count(*) AS n FROM {TBL_ENTITIES}").collect()[0]["n"]
an = spark.sql(f"SELECT count(*) AS n FROM {TBL_ENTITY_TIMELINE} WHERE anomaly_flag=true").collect()[0]["n"]
tr = spark.sql(f"""
    SELECT trend, count(*) AS n FROM {TBL_ENTITY_TIMELINE}
    GROUP BY trend ORDER BY n DESC
""").collect()

print(f"\n=== ARCAS Graph Enrichment L2 ===")
print(f"Entidades raw:        {ta}")
print(f"Entidades timeline:   {te}")
print(f"Anomalias detectadas: {an}")
print(f"Tendencias:")
for row in tr:
    print(f"  {row['trend']}: {row['n']}")
print("=================================")
