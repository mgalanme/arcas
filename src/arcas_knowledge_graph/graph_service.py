"""
ARCAS - src/arcas_knowledge_graph/graph_service.py

Neo4j graph service: manages actor/organisation nodes and relationships.
Implements entity reconciliation with confidence-based HITL routing.
"""
import logging, os
from typing import Optional
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
log = logging.getLogger(__name__)

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
HIGH_CONF      = 0.90   # Above this: auto-merge. Below: HITL queue.


class GraphService:
    """
    Manages the ARCAS knowledge graph in Neo4j.
    All write operations use pseudonymised tokens as node identifiers.
    Real identifiers never enter this service.
    """

    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # ------------------------------------------------------------------
    # Actor operations
    # ------------------------------------------------------------------

    def upsert_actor(self, actor_id: str, actor_type: str, display_name: str,
                     metadata: dict = None) -> bool:
        """Create or update an Actor node. Returns True if created, False if updated."""
        with self.driver.session() as s:
            result = s.run("""
                MERGE (a:Actor {actor_id: $actor_id})
                ON CREATE SET
                    a.actor_type    = $actor_type,
                    a.display_name  = $display_name,
                    a.metadata      = $metadata,
                    a.created_at    = datetime(),
                    a.last_updated  = datetime(),
                    a.risk_score_global = 0.0
                ON MATCH SET
                    a.last_updated  = datetime()
                RETURN a.actor_id, (a.created_at = a.last_updated) AS is_new
            """, actor_id=actor_id, actor_type=actor_type,
                 display_name=display_name, metadata=str(metadata or {}))
            record = result.single()
            return bool(record and record["is_new"])

    def upsert_relationship(self, source_id: str, target_id: str,
                             rel_type: str, properties: dict = None) -> None:
        """Create or update a relationship between two actors."""
        props = properties or {}
        with self.driver.session() as s:
            s.run("""
                MATCH (a:Actor {actor_id: $src})
                MATCH (b:Actor {actor_id: $tgt})
                MERGE (a)-[r:RELATED_TO {rel_type: $rel_type}]->(b)
                ON CREATE SET r.strength = 1.0, r.created_at = datetime(), r += $props
                ON MATCH  SET r.strength = r.strength + 0.1, r.last_seen = datetime()
            """, src=source_id, tgt=target_id, rel_type=rel_type, props=props)

    def find_similar_actors(self, display_name: str, actor_type: str,
                             limit: int = 5) -> list[dict]:
        """Full-text search for similar existing actors (used in reconciliation)."""
        with self.driver.session() as s:
            result = s.run("""
                CALL db.index.fulltext.queryNodes('actor_name_search', $query)
                YIELD node, score
                WHERE node.actor_type = $actor_type
                RETURN node.actor_id AS actor_id,
                       node.display_name AS display_name,
                       score
                ORDER BY score DESC
                LIMIT $limit
            """, query=display_name, actor_type=actor_type, limit=limit)
            return [dict(r) for r in result]

    def update_risk_score(self, actor_id: str, category: str, score: float) -> None:
        """Update a category-specific risk score and recompute the global score."""
        prop = f"risk_score_cat_{category.lower()}"
        with self.driver.session() as s:
            s.run(f"""
                MATCH (a:Actor {{actor_id: $actor_id}})
                SET a.{prop} = $score,
                    a.risk_score_global = (
                        coalesce(a.risk_score_cat_a, 0) +
                        coalesce(a.risk_score_cat_b, 0) +
                        coalesce(a.risk_score_cat_c, 0) +
                        coalesce(a.risk_score_cat_d, 0) +
                        coalesce(a.risk_score_cat_e, 0) +
                        coalesce(a.risk_score_cat_f, 0)
                    ) / 6.0,
                    a.last_updated = datetime()
            """, actor_id=actor_id, score=score)

    def get_actor_network(self, actor_id: str, depth: int = 2) -> dict:
        """Return the ego network of an actor up to the given depth."""
        with self.driver.session() as s:
            result = s.run("""
                MATCH path = (a:Actor {actor_id: $actor_id})-[*1..$depth]-(b:Actor)
                RETURN nodes(path) AS nodes, relationships(path) AS rels
            """, actor_id=actor_id, depth=depth)
            nodes, edges = set(), []
            for record in result:
                for node in record["nodes"]:
                    nodes.add((node["actor_id"], node.get("display_name", ""), node.get("actor_type", "")))
                for rel in record["rels"]:
                    edges.append({"type": rel.get("rel_type", ""), "strength": rel.get("strength", 1.0)})
            return {"nodes": list(nodes), "edges": edges}

    def close(self):
        self.driver.close()
