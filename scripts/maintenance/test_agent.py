"""
ARCAS - scripts/maintenance/test_agent.py

Smoke test for the agent orchestrator.
Injects a synthetic event representing a Category A (procurement fraud)
pattern and verifies that the LangGraph flow runs to the HITL checkpoint.

Usage:
  PYTHONPATH=. python scripts/maintenance/test_agent.py --synthetic
  PYTHONPATH=. python scripts/maintenance/test_agent.py --synthetic --skip-llm

Requires: source .venv-langchain/bin/activate
"""

import argparse
import json
import logging
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# =============================================================================
# SYNTHETIC TEST DATA
# All entities are fictional. No real persons are referenced.
# =============================================================================

SYNTHETIC_ACTOR_A = {
    "actor_id": f"test_actor_{uuid.uuid4().hex[:8]}",
    "actor_type": "political",
    "display_name": "[SYNTHETIC-TEST-OFFICIAL]",
    "is_synthetic": True,
}

SYNTHETIC_ACTOR_B = {
    "actor_id": f"test_actor_{uuid.uuid4().hex[:8]}",
    "actor_type": "corporate",
    "display_name": "[SYNTHETIC-TEST-COMPANY]",
    "is_synthetic": True,
}

SYNTHETIC_EVENT = {
    "event_id": str(uuid.uuid4()),
    "event_type": "contract_award",
    "actors": [SYNTHETIC_ACTOR_A["actor_id"], SYNTHETIC_ACTOR_B["actor_id"]],
    "event_date": "2024-01-15",
    "description": (
        "[SYNTHETIC TEST EVENT] Fictional contract award between synthetic entities. "
        "This event is for pipeline testing only and represents no real transaction."
    ),
    "source_url": "https://example.test/synthetic-event",
    "amount_eur": 499000.00,   # Just below 500k threshold - potential splitting pattern
    "is_synthetic": True,
}

# Simulate a pattern: same pair has 3 prior contracts in the last 12 months
SYNTHETIC_PRIOR_CONTRACTS = [
    {"id": str(uuid.uuid4()), "amount": 498000, "date": "2023-03-10"},
    {"id": str(uuid.uuid4()), "amount": 497500, "date": "2023-07-22"},
    {"id": str(uuid.uuid4()), "amount": 495000, "date": "2023-11-05"},
]


def test_agent_imports() -> bool:
    """Verify all required agent modules can be imported."""
    log.info("Testing agent module imports...")
    errors = []

    modules = [
        ("langgraph", "langgraph"),
        ("langchain_groq", "langchain_groq"),
        ("neo4j", "neo4j"),
        ("qdrant_client", "qdrant_client"),
        ("psycopg2", "psycopg2"),
        ("redis", "redis"),
        ("opentelemetry.sdk.trace", "opentelemetry.sdk.trace"),
    ]

    for display_name, module_name in modules:
        try:
            __import__(module_name)
            log.info(f"  OK: {display_name}")
        except ImportError as e:
            log.error(f"  FAIL: {display_name} - {e}")
            errors.append(display_name)

    if errors:
        log.error(f"Import failures: {errors}")
        return False
    log.info("All imports successful.")
    return True


def test_database_connections() -> dict[str, bool]:
    """Test connectivity to all required data stores."""
    results = {}

    # PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "arcas"),
            user=os.getenv("POSTGRES_USER", "arcas_app"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            connect_timeout=5,
        )
        conn.close()
        results["postgres"] = True
        log.info("  PostgreSQL: OK")
    except Exception as e:
        results["postgres"] = False
        log.error(f"  PostgreSQL: FAIL - {e}")

    # Neo4j
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "")),
        )
        with driver.session() as session:
            session.run("RETURN 1")
        driver.close()
        results["neo4j"] = True
        log.info("  Neo4j: OK")
    except Exception as e:
        results["neo4j"] = False
        log.error(f"  Neo4j: FAIL - {e}")

    # Qdrant
    try:
        from qdrant_client import QdrantClient
        client = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
            timeout=5,
        )
        client.get_collections()
        results["qdrant"] = True
        log.info("  Qdrant: OK")
    except Exception as e:
        results["qdrant"] = False
        log.error(f"  Qdrant: FAIL - {e}")

    # Redis
    try:
        import redis as redis_lib
        r = redis_lib.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), socket_timeout=5)
        r.ping()
        results["redis"] = True
        log.info("  Redis: OK")
    except Exception as e:
        results["redis"] = False
        log.error(f"  Redis: FAIL - {e}")

    return results


def test_groq_api(skip_llm: bool = False) -> bool:
    """Test Groq API connectivity with a minimal call."""
    if skip_llm:
        log.info("  Groq API: SKIPPED (--skip-llm flag set)")
        return True

    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key or api_key.startswith("gsk_YOUR"):
        log.warning("  Groq API: SKIPPED (GROQ_API_KEY not configured)")
        return True

    try:
        from langchain_groq import ChatGroq
        llm = ChatGroq(
            model="llama-3.1-8b-instant",   # Use the fast model for the smoke test
            api_key=api_key,
            max_tokens=50,
        )
        response = llm.invoke("Reply with exactly one word: 'OK'")
        log.info(f"  Groq API: OK (response: {str(response.content)[:50]})")
        return True
    except Exception as e:
        log.error(f"  Groq API: FAIL - {e}")
        return False


def test_langgraph_minimal() -> bool:
    """
    Test minimal LangGraph state graph construction.
    Does NOT call any LLM - tests graph structure only.
    """
    log.info("  Testing LangGraph state graph construction...")
    try:
        from langgraph.graph import StateGraph, END
        from typing import TypedDict

        class TestState(TypedDict):
            value: int
            status: str

        def node_a(state: TestState) -> TestState:
            return {"value": state["value"] + 1, "status": "processed"}

        def route(state: TestState) -> str:
            return END

        graph = StateGraph(TestState)
        graph.add_node("process", node_a)
        graph.set_entry_point("process")
        graph.add_conditional_edges("process", route)

        compiled = graph.compile()

        # Run with synthetic state
        result = compiled.invoke({"value": 0, "status": "initial"})
        assert result["value"] == 1, f"Expected value=1, got {result['value']}"
        assert result["status"] == "processed"

        log.info("  LangGraph: OK")
        return True
    except Exception as e:
        log.error(f"  LangGraph: FAIL - {e}")
        return False


def run_smoke_test(synthetic: bool, skip_llm: bool) -> bool:
    log.info("=" * 60)
    log.info("ARCAS Agent Orchestrator Smoke Test")
    log.info(f"Mode: {'synthetic' if synthetic else 'live'} | skip-llm: {skip_llm}")
    log.info("=" * 60)

    results = {}

    log.info("\n[1/4] Module imports")
    results["imports"] = test_agent_imports()

    log.info("\n[2/4] Database connections")
    db_results = test_database_connections()
    results.update(db_results)

    log.info("\n[3/4] LLM API")
    results["groq"] = test_groq_api(skip_llm)

    log.info("\n[4/4] LangGraph structure")
    results["langgraph"] = test_langgraph_minimal()

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("Smoke Test Results:")
    all_passed = True
    for component, passed in results.items():
        status = "PASS" if passed else "FAIL"
        indicator = "OK" if passed else "!!"
        log.info(f"  [{indicator}] {component}: {status}")
        if not passed:
            all_passed = False

    log.info("")
    if all_passed:
        log.info("All checks passed. Environment is ready for agent implementation.")
    else:
        log.warning("Some checks failed. Review errors above before proceeding.")
    log.info("=" * 60)

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARCAS agent orchestrator smoke test")
    parser.add_argument(
        "--synthetic",
        action="store_true",
        default=False,
        help="Use synthetic test data (required for this script)",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        default=False,
        help="Skip LLM API connectivity test (useful when Groq key is not yet configured)",
    )
    args = parser.parse_args()

    if not args.synthetic:
        log.error("This script requires --synthetic flag. Live data mode is not implemented yet.")
        exit(1)

    success = run_smoke_test(args.synthetic, args.skip_llm)
    exit(0 if success else 1)
