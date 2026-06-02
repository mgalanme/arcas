"""
ARCAS - src/arcas_api/routers/alerts.py  (fix C-14)

C-14 fix: adds GET /api/v1/actors/{actor_id}/network endpoint
used by network_explorer.py in the Streamlit dashboard.
"""
import logging, os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException, Query
from neo4j import GraphDatabase

router = APIRouter()
log    = logging.getLogger(__name__)

PG_DSN = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")


def get_pg():
    return psycopg2.connect(PG_DSN)


def get_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ---------------------------------------------------------------------------
# Alert endpoints
# ---------------------------------------------------------------------------

@router.get("/")
async def list_alerts(
    status:   Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit:    int           = Query(50, le=200),
    offset:   int           = Query(0),
):
    where_clauses, params = [], []
    if status:
        where_clauses.append("status = %s")
        params.append(status)
    if category:
        where_clauses.append("category = %s")
        params.append(category)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    conn = get_pg()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM alerts {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                params + [limit, offset]
            )
            rows = cur.fetchall()
            return {"alerts": [dict(r) for r in rows], "limit": limit, "offset": offset}
    finally:
        conn.close()


@router.get("/{alert_id}")
async def get_alert(alert_id: str):
    conn = get_pg()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Alert not found")
            return dict(row)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# C-14 fix: actor network endpoint used by network_explorer.py
# ---------------------------------------------------------------------------

@router.get("/actors/{actor_id}/network")
async def get_actor_network(
    actor_id: str,
    depth:    int = Query(2, ge=1, le=3),
):
    """
    Returns the ego network of an actor up to the given depth.
    Used by the Streamlit Network Explorer page.
    Returns: {nodes: [[actor_id, display_name, actor_type], ...], edges: [...]}
    """
    driver = get_neo4j()
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH path = (a:Actor {actor_id: $actor_id})-[*1..$depth]-(b:Actor)
                RETURN
                    [n IN nodes(path) |
                        [n.actor_id, coalesce(n.display_name, n.actor_id), coalesce(n.actor_type, 'other')]
                    ] AS node_list,
                    [r IN relationships(path) |
                        {type: coalesce(r.rel_type, type(r)),
                         strength: coalesce(r.strength, 1.0),
                         source: startNode(r).actor_id,
                         target: endNode(r).actor_id}
                    ] AS edge_list
            """, actor_id=actor_id, depth=depth)

            nodes_seen = set()
            nodes      = []
            edges      = []

            for record in result:
                for node_tuple in record["node_list"]:
                    if node_tuple[0] not in nodes_seen:
                        nodes_seen.add(node_tuple[0])
                        nodes.append({
                            "actor_id":     node_tuple[0],
                            "display_name": node_tuple[1],
                            "actor_type":   node_tuple[2],
                        })
                for edge in record["edge_list"]:
                    edges.append(edge)

            if not nodes:
                # Return the actor itself if no connections found
                with driver.session() as s2:
                    r2 = s2.run(
                        "MATCH (a:Actor {actor_id: $id}) RETURN a",
                        id=actor_id
                    )
                    rec = r2.single()
                    if rec:
                        a = rec["a"]
                        nodes = [{
                            "actor_id":     a["actor_id"],
                            "display_name": a.get("display_name", actor_id),
                            "actor_type":   a.get("actor_type", "other"),
                        }]
                    else:
                        raise HTTPException(status_code=404, detail="Actor not found")

            return {"actor_id": actor_id, "depth": depth, "nodes": nodes, "edges": edges}
    finally:
        driver.close()


@router.get("/actors/")
async def list_actors(
    limit:  int           = Query(50, le=200),
    search: Optional[str] = Query(None),
):
    """List actors from Neo4j. Optionally filter by display_name."""
    driver = get_neo4j()
    try:
        with driver.session() as session:
            if search:
                result = session.run("""
                    MATCH (a:Actor)
                    WHERE toLower(a.display_name) CONTAINS toLower($search)
                    RETURN a.actor_id AS actor_id,
                           a.display_name AS display_name,
                           a.actor_type AS actor_type,
                           a.risk_score_global AS risk_score
                    ORDER BY a.risk_score_global DESC
                    LIMIT $limit
                """, search=search, limit=limit)
            else:
                result = session.run("""
                    MATCH (a:Actor)
                    RETURN a.actor_id AS actor_id,
                           a.display_name AS display_name,
                           a.actor_type AS actor_type,
                           a.risk_score_global AS risk_score
                    ORDER BY a.risk_score_global DESC
                    LIMIT $limit
                """, limit=limit)
            return {"actors": [dict(r) for r in result]}
    finally:
        driver.close()
