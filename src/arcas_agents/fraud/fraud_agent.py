"""
ARCAS - src/arcas_agents/fraud/fraud_agent.py

CrewAI specialist agent: Category A - Public Procurement Fraud Detection.
Analyses contract patterns, identifies circular rotation, splitting,
conflict of interest and donation-award correlations.
"""
import logging, os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

load_dotenv()
log = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL_PRIMARY", "llama-3.3-70b-versatile")


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

class ContractQueryInput(BaseModel):
    entity_id:  str  = Field(description="Pseudonymous actor token to query contracts for")
    date_range: str  = Field(description="Date range as YYYY-MM-DD:YYYY-MM-DD")

class GraphQueryInput(BaseModel):
    cypher: str = Field(description="Cypher query to execute against the knowledge graph")


# ---------------------------------------------------------------------------
# Tools (stubs - connect to graph_service and PostgreSQL in production)
# ---------------------------------------------------------------------------

class ContractQueryTool(BaseTool):
    name:        str = "query_contracts"
    description: str = "Query public contracts awarded to or by a specific actor"
    args_schema       = ContractQueryInput

    def _run(self, entity_id: str, date_range: str) -> str:
        # Production: query PostgreSQL events table filtered by entity_id
        return f"[STUB] Contract query for {entity_id} in {date_range}: No live data in stub mode"


class GraphQueryTool(BaseTool):
    name:        str = "query_knowledge_graph"
    description: str = "Execute a Cypher query on the Neo4j knowledge graph"
    args_schema       = GraphQueryInput

    def _run(self, cypher: str) -> str:
        # Production: connect to graph_service and execute Cypher
        return f"[STUB] Graph query: {cypher[:100]}"


# ---------------------------------------------------------------------------
# Agent and crew definition
# ---------------------------------------------------------------------------

def create_fraud_crew(event_data: dict) -> Crew:
    """Create a CrewAI crew for procurement fraud analysis."""

    llm_config = {"model": f"groq/{GROQ_MODEL}", "api_key": GROQ_API_KEY}

    fraud_analyst = Agent(
        role="Public Procurement Fraud Analyst",
        goal=(
            "Identify statistically significant patterns in public contract data "
            "that may indicate fraud, corruption or abuse of procurement rules. "
            "Base analysis ONLY on publicly available data. Never accuse - only identify patterns."
        ),
        backstory=(
            "You are an expert in public sector procurement with deep knowledge of "
            "Spanish and EU contracting law. You detect anomalies by analysing contract "
            "award patterns, amounts, beneficiaries and timing correlations."
        ),
        tools=[ContractQueryTool(), GraphQueryTool()],
        llm=llm_config,
        verbose=False,
        max_iter=5,
    )

    analysis_task = Task(
        description=(
            f"Analyse the following procurement event for fraud patterns:\n"
            f"{str(event_data)[:1500]}\n\n"
            "Check for: contract splitting, circular rotation, conflict of interest, "
            "and unusual award patterns. Provide a structured analysis with: "
            "1) Pattern detected (or none) 2) Supporting evidence from the data "
            "3) Confidence score 0-1 4) Recommended action"
        ),
        expected_output=(
            "A structured analysis with pattern type, evidence, confidence score, "
            "and recommended action. Be factual and concise."
        ),
        agent=fraud_analyst,
    )

    return Crew(
        agents=[fraud_analyst],
        tasks=[analysis_task],
        process=Process.sequential,
        verbose=False,
    )


def run_fraud_analysis(event_data: dict) -> dict:
    """Run the fraud analysis crew and return the result."""
    crew = create_fraud_crew(event_data)
    result = crew.kickoff()
    return {
        "agent":    "fraud_agent",
        "category": "A",
        "output":   str(result),
    }
