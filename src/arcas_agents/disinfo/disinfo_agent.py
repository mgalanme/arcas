"""
ARCAS - src/arcas_agents/disinfo/disinfo_agent.py
CrewAI specialist: Category D - Disinformation Detection and Beneficiary Analysis.
Detects false claims, narrative coordination and identifies who benefits.
"""
import logging, os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL_PRIMARY", "llama-3.3-70b-versatile")


class ClaimVerifyInput(BaseModel):
    claim: str    = Field(description="The claim to verify against public records")
    sources: str  = Field(description="Comma-separated source URLs to check against")


class SemanticSearchInput(BaseModel):
    query: str    = Field(description="Natural language search query")
    filters: str  = Field(description="JSON filter: date_range, source_type")


class ClaimVerifyTool(BaseTool):
    name = "verify_claim"
    description = "Cross-reference a claim against verified public records"
    args_schema = ClaimVerifyInput
    def _run(self, claim: str, sources: str) -> str:
        return f"[STUB] Claim verification: '{claim[:80]}': unverified"


class SemanticSearchTool(BaseTool):
    name = "search_evidence"
    description = "Semantic search over the evidence base in Qdrant"
    args_schema = SemanticSearchInput
    def _run(self, query: str, filters: str) -> str:
        return f"[STUB] Semantic search: '{query[:60]}': no results in stub mode"


def run_disinfo_analysis(event_data: dict) -> dict:
    llm = {"model": f"groq/{GROQ_MODEL}", "api_key": GROQ_API_KEY}
    analyst = Agent(
        role="Disinformation Analyst",
        goal="Detect false or misleading claims in media content, identify narrative coordination across outlets, and determine who benefits from the disinformation.",
        backstory="Expert in media analysis and fact-checking with focus on politically motivated disinformation.",
        tools=[ClaimVerifyTool(), SemanticSearchTool()],
        llm=llm, verbose=False, max_iter=5,
    )
    task = Task(
        description=f"Analyse for disinformation patterns: {str(event_data)[:1000]}. "
                    "Identify: false claims, coordinated narratives, beneficiary actors.",
        expected_output="Disinformation analysis: claim status, coordination score, beneficiary mapping.",
        agent=analyst,
    )
    result = Crew(agents=[analyst], tasks=[task], process=Process.sequential, verbose=False).kickoff()
    return {"agent": "disinfo_agent", "category": "D", "output": str(result)}
