"""
ARCAS - scripts/setup/init_neo4j_schema.py

Full Neo4j schema initialisation:
- Uniqueness constraints for all node types
- Indexes for frequent lookup patterns
- Full-text search indexes for entity name search
- APOC verification

Run with: PYTHONPATH=. python scripts/setup/init_neo4j_schema.py
Requires: source .venv-langchain/bin/activate
"""

import os
import sys
import time
import logging

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, ClientError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not NEO4J_PASSWORD:
    log.error("NEO4J_PASSWORD not set. Check your .env file.")
    sys.exit(1)

# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

CONSTRAINTS = [
    # Node uniqueness constraints
    "CREATE CONSTRAINT actor_id IF NOT EXISTS FOR (a:Actor) REQUIRE a.actor_id IS UNIQUE",
    "CREATE CONSTRAINT org_id IF NOT EXISTS FOR (o:Organisation) REQUIRE o.org_id IS UNIQUE",
    "CREATE CONSTRAINT evidence_id IF NOT EXISTS FOR (e:Evidence) REQUIRE e.evidence_id IS UNIQUE",
    "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (ev:Event) REQUIRE ev.event_id IS UNIQUE",
    "CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (al:Alert) REQUIRE al.alert_id IS UNIQUE",
    "CREATE CONSTRAINT disinfo_id IF NOT EXISTS FOR (d:DisinfoRecord) REQUIRE d.record_id IS UNIQUE",
    "CREATE CONSTRAINT judicial_id IF NOT EXISTS FOR (j:JudicialPattern) REQUIRE j.pattern_id IS UNIQUE",
    "CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:Source) REQUIRE s.source_id IS UNIQUE",
]

INDEXES = [
    # Actor indexes
    "CREATE INDEX actor_type IF NOT EXISTS FOR (a:Actor) ON (a.actor_type)",
    "CREATE INDEX actor_risk IF NOT EXISTS FOR (a:Actor) ON (a.risk_score_global)",
    "CREATE INDEX actor_updated IF NOT EXISTS FOR (a:Actor) ON (a.last_updated)",

    # Organisation indexes
    "CREATE INDEX org_type IF NOT EXISTS FOR (o:Organisation) ON (o.org_type)",
    "CREATE INDEX org_jurisdiction IF NOT EXISTS FOR (o:Organisation) ON (o.jurisdiction)",
    "CREATE INDEX org_sector IF NOT EXISTS FOR (o:Organisation) ON (o.sector)",

    # Event indexes
    "CREATE INDEX event_date IF NOT EXISTS FOR (ev:Event) ON (ev.event_date)",
    "CREATE INDEX event_type IF NOT EXISTS FOR (ev:Event) ON (ev.event_type)",

    # Alert indexes
    "CREATE INDEX alert_status IF NOT EXISTS FOR (al:Alert) ON (al.status)",
    "CREATE INDEX alert_category IF NOT EXISTS FOR (al:Alert) ON (al.category)",
    "CREATE INDEX alert_confidence IF NOT EXISTS FOR (al:Alert) ON (al.confidence_score)",
    "CREATE INDEX alert_created IF NOT EXISTS FOR (al:Alert) ON (al.created_at)",

    # DisinfoRecord indexes
    "CREATE INDEX disinfo_outlet IF NOT EXISTS FOR (d:DisinfoRecord) ON (d.outlet_id)",
    "CREATE INDEX disinfo_status IF NOT EXISTS FOR (d:DisinfoRecord) ON (d.verification_status)",
    "CREATE INDEX disinfo_date IF NOT EXISTS FOR (d:DisinfoRecord) ON (d.pub_date)",

    # JudicialPattern indexes
    "CREATE INDEX judicial_judge IF NOT EXISTS FOR (j:JudicialPattern) ON (j.judge_id)",
    "CREATE INDEX judicial_type IF NOT EXISTS FOR (j:JudicialPattern) ON (j.pattern_type)",

    # Relationship indexes
    "CREATE INDEX rel_type IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.rel_type)",
    "CREATE INDEX rel_date_from IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.date_from)",
    "CREATE INDEX rel_strength IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.strength)",
]

FULLTEXT_INDEXES = [
    # Full-text search for entity name lookup (used by reconciliation agent)
    """
    CREATE FULLTEXT INDEX actor_name_search IF NOT EXISTS
    FOR (a:Actor) ON EACH [a.display_name, a.aliases]
    """,
    """
    CREATE FULLTEXT INDEX org_name_search IF NOT EXISTS
    FOR (o:Organisation) ON EACH [o.display_name, o.aliases, o.legal_name]
    """,
    """
    CREATE FULLTEXT INDEX evidence_text_search IF NOT EXISTS
    FOR (e:Evidence) ON EACH [e.pseudonymised_text, e.title]
    """,
]

ONTOLOGY_SETUP = [
    # Create a metadata node to store the ontology version
    """
    MERGE (meta:ArcasMetadata {id: 'schema_version'})
    SET meta.version = '1.0',
        meta.created_at = datetime(),
        meta.last_updated = datetime()
    RETURN meta
    """,
]

# Actor types that the system recognises
ACTOR_TYPES = [
    "political",        # Elected officials, party members
    "corporate",        # Business executives, board members
    "judicial",         # Judges, prosecutors, magistrates
    "law_enforcement",  # Police officers, investigators
    "legal",            # Lawyers, notaries with public-sector involvement
    "media",            # Journalists, columnists, media owners
    "regulator",        # Regulatory and inspection bodies
    "think_tank",       # Think tank and foundation members
    "influencer",       # Digital influencers in public-interest debates
    "other",            # Catch-all for unclassified actors
]

# Relationship types in the ontology
RELATIONSHIP_TYPES = [
    "EMPLOYMENT",            # Works for / worked for
    "OWNERSHIP",             # Owns / owned
    "CONTRACT_AWARD",        # Awarded contract to / received contract from
    "FINANCING",             # Financed / received financing from
    "MEDIA_COVERAGE",        # Covered / was covered by (Actor -> Source)
    "DECLARED_FAMILY",       # Declared family relationship (from public registries)
    "LEGAL_REPRESENTATION",  # Represented legally by / legally represented
    "PARTY_MEMBERSHIP",      # Member of political party
    "REGULATORY_AUTHORITY",  # Regulates / is regulated by
    "INVESTIGATED_BY",       # Under investigation by
    "COLLABORATED_WITH",     # General collaboration relationship
    "NOMINATED_BY",          # Nominated / appointed by
]

# =============================================================================
# MAIN
# =============================================================================

def wait_for_neo4j(driver: GraphDatabase.driver, max_retries: int = 12) -> bool:
    """Wait until Neo4j is ready to accept connections."""
    for attempt in range(1, max_retries + 1):
        try:
            with driver.session() as session:
                session.run("RETURN 1")
            log.info("Neo4j is ready.")
            return True
        except ServiceUnavailable:
            log.info(f"Neo4j not yet ready (attempt {attempt}/{max_retries})...")
            time.sleep(10)
    return False


def verify_apoc(driver: GraphDatabase.driver) -> bool:
    """Verify APOC is installed and working."""
    try:
        with driver.session() as session:
            result = session.run("CALL apoc.help('apoc') YIELD name RETURN count(*) as n")
            count = result.single()["n"]
            log.info(f"APOC verified: {count} procedures available.")
            return True
    except ClientError as e:
        log.error(f"APOC not available: {e}")
        log.error("Ensure NEO4J_PLUGINS='[\"apoc\"]' is set in the container environment.")
        return False


def run_statements(driver: GraphDatabase.driver, statements: list[str], label: str) -> None:
    """Execute a list of Cypher statements, continuing on non-fatal errors."""
    success = 0
    skipped = 0
    failed  = 0

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            with driver.session() as session:
                session.run(stmt)
            success += 1
        except ClientError as e:
            if "already exists" in str(e).lower():
                skipped += 1
            else:
                log.warning(f"Statement failed ({label}): {e}")
                log.warning(f"Statement: {stmt[:120]}...")
                failed += 1

    log.info(f"{label}: {success} created, {skipped} already existed, {failed} failed")


def main() -> None:
    log.info("=" * 60)
    log.info("ARCAS - Neo4j Schema Initialisation")
    log.info(f"URI: {NEO4J_URI}")
    log.info("=" * 60)

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        connection_timeout=10,
        max_connection_lifetime=3600,
    )

    try:
        # Wait for Neo4j to be ready
        if not wait_for_neo4j(driver):
            log.error("Neo4j did not become ready. Aborting.")
            sys.exit(1)

        # Verify APOC
        if not verify_apoc(driver):
            log.warning("Proceeding without APOC verification. Some features may not work.")

        # Apply schema
        log.info("")
        log.info("Applying uniqueness constraints...")
        run_statements(driver, CONSTRAINTS, "Constraints")

        log.info("Creating indexes...")
        run_statements(driver, INDEXES, "Indexes")

        log.info("Creating full-text indexes...")
        run_statements(driver, FULLTEXT_INDEXES, "Full-text indexes")

        log.info("Setting up ontology metadata...")
        run_statements(driver, ONTOLOGY_SETUP, "Ontology setup")

        # Verify final state
        log.info("")
        log.info("Verifying schema...")
        with driver.session() as session:
            constraints = session.run("SHOW CONSTRAINTS YIELD name RETURN count(*) as n").single()["n"]
            indexes     = session.run("SHOW INDEXES YIELD name RETURN count(*) as n").single()["n"]
            log.info(f"  Constraints: {constraints}")
            log.info(f"  Indexes:     {indexes}")

        log.info("")
        log.info("=" * 60)
        log.info("Neo4j schema initialisation complete.")
        log.info("")
        log.info("Ontology summary:")
        log.info(f"  Actor types: {len(ACTOR_TYPES)}")
        log.info(f"  Relationship types: {len(RELATIONSHIP_TYPES)}")
        log.info("=" * 60)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
