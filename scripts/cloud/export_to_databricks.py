"""
ARCAS - scripts/cloud/export_to_databricks.py
Seeds Databricks Delta Lake with data from the local stack.
"""
import json, logging, os, tempfile, pathlib, base64
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Hard-coded to avoid .env parsing issues
HOST  = "https://dbc-d9cdf0a6-0761.cloud.databricks.com"
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
WH_ID = "35c29c1124e50bcc"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

KAFKA   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
PG_CONN = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER","neo4j"), os.getenv("NEO4J_PASSWORD",""))


def sql(statement: str) -> dict:
    """Execute one SQL statement and return the result."""
    resp = requests.post(
        f"{HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json={"warehouse_id": WH_ID, "statement": statement, "wait_timeout": "50s"},
    )
    resp.raise_for_status()
    result = resp.json()
    state  = result.get("status", {}).get("state", "?")
    log.info(f"  [{state}] {statement[:80]}")
    return result


def upload(local_path: str, dbfs_path: str) -> None:
    """Upload a file to DBFS."""
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()
    resp = requests.post(
        f"{HOST}/api/2.0/dbfs/put",
        headers=HEADERS,
        json={"path": dbfs_path, "contents": content, "overwrite": True},
    )
    resp.raise_for_status()
    log.info(f"  Uploaded → {dbfs_path}")


def write_ndjson(records: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            clean = {}
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    clean[k] = v.isoformat()
                elif isinstance(v, (list, dict)):
                    clean[k] = json.dumps(v, ensure_ascii=False)
                else:
                    clean[k] = v
            f.write(json.dumps(clean, ensure_ascii=False) + "\n")


def kafka_read(topic: str, limit: int = 2000) -> list[dict]:
    log.info(f"Reading Kafka topic: {topic}")
    c = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA],
        auto_offset_reset="earliest",
        consumer_timeout_ms=8000,
        value_deserializer=lambda v: json.loads(v.decode()),
        group_id=f"arcas-dbx-export",
    )
    rows = []
    for m in c:
        rows.append(m.value)
        if len(rows) >= limit:
            break
    c.close()
    log.info(f"  Read {len(rows)} records")
    return rows


def pg_read(query: str) -> list[dict]:
    conn = psycopg2.connect(PG_CONN)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def neo4j_read() -> list[dict]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as s:
        result = s.run("""
            MATCH (a:Actor)
            RETURN a.actor_id AS actor_id,
                   a.display_name AS display_name,
                   a.actor_type AS actor_type,
                   a.risk_score_global AS risk_score_global,
                   toString(a.first_detected) AS first_detected,
                   toString(a.last_updated) AS last_updated
        """)
        rows = [dict(r) for r in result]
    driver.close()
    log.info(f"  Read {len(rows)} actors from Neo4j")
    return rows


def load_table(name: str, dbfs_path: str, ddl: str) -> None:
    """Create table and load NDJSON from DBFS."""
    sql(ddl)
    sql(f"""
        COPY INTO {name}
        FROM '{dbfs_path}'
        FILEFORMAT = JSON
        FORMAT_OPTIONS ('inferSchema' = 'true', 'mergeSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)


def main():
    tmp = pathlib.Path(tempfile.mkdtemp())
    log.info("=== ARCAS → Databricks export ===")

    # Schemas already created in previous run
    log.info("Ensuring schemas exist...")
    sql("CREATE DATABASE IF NOT EXISTS arcas_raw")
    sql("CREATE DATABASE IF NOT EXISTS arcas_processed")

    # --- Articles ---
    log.info("--- Articles (media + gazette) ---")
    articles = kafka_read("arcas.raw.media") + kafka_read("arcas.raw.gazette")
    if articles:
        p = str(tmp / "articles.ndjson")
        write_ndjson(articles, p)
        upload(p, "/FileStore/arcas/articles.ndjson")
        load_table(
            "arcas_raw.articles",
            "dbfs:/FileStore/arcas/articles.ndjson",
            """CREATE TABLE IF NOT EXISTS arcas_raw.articles (
                source_type STRING, source_name STRING, title STRING,
                content_url STRING, pub_date STRING, language STRING,
                jurisdiction STRING, content_hash STRING,
                is_synthetic BOOLEAN, is_factchecker BOOLEAN
            ) USING DELTA""",
        )
        log.info(f"  Loaded {len(articles)} articles")

    # --- Fact-checkers ---
    log.info("--- Fact-checkers ---")
    fcs = kafka_read("arcas.raw.factcheck")
    if fcs:
        p = str(tmp / "factchecks.ndjson")
        write_ndjson(fcs, p)
        upload(p, "/FileStore/arcas/factchecks.ndjson")
        load_table(
            "arcas_raw.factchecks",
            "dbfs:/FileStore/arcas/factchecks.ndjson",
            """CREATE TABLE IF NOT EXISTS arcas_raw.factchecks (
                source_name STRING, title STRING, content_url STRING,
                language STRING, content_hash STRING, is_synthetic BOOLEAN
            ) USING DELTA""",
        )
        log.info(f"  Loaded {len(fcs)} fact-check records")

    # --- Alerts ---
    log.info("--- Alerts ---")
    alerts = pg_read("SELECT * FROM alerts ORDER BY created_at DESC")
    if alerts:
        p = str(tmp / "alerts.ndjson")
        write_ndjson(alerts, p)
        upload(p, "/FileStore/arcas/alerts.ndjson")
        load_table(
            "arcas_processed.alerts",
            "dbfs:/FileStore/arcas/alerts.ndjson",
            """CREATE TABLE IF NOT EXISTS arcas_processed.alerts (
                alert_id STRING, category STRING, status STRING,
                confidence_score DOUBLE, nl_justification STRING,
                reasoning_chain STRING, metadata STRING,
                created_at STRING, updated_at STRING
            ) USING DELTA""",
        )
        log.info(f"  Loaded {len(alerts)} alerts")

    # --- Actors ---
    log.info("--- Actors ---")
    actors = neo4j_read()
    if actors:
        p = str(tmp / "actors.ndjson")
        write_ndjson(actors, p)
        upload(p, "/FileStore/arcas/actors.ndjson")
        load_table(
            "arcas_processed.actors",
            "dbfs:/FileStore/arcas/actors.ndjson",
            """CREATE TABLE IF NOT EXISTS arcas_processed.actors (
                actor_id STRING, display_name STRING, actor_type STRING,
                risk_score_global DOUBLE, first_detected STRING, last_updated STRING
            ) USING DELTA""",
        )
        log.info(f"  Loaded {len(actors)} actors")

    log.info("=== Export complete ===")
    log.info("Tables in Databricks:")
    log.info("  arcas_raw.articles  arcas_raw.factchecks")
    log.info("  arcas_processed.alerts  arcas_processed.actors")


if __name__ == "__main__":
    main()
