"""
ARCAS - src/arcas_agents/judicial/judicial_agent.py
CrewAI specialist: Category C - Judicial Pattern Analysis.
Analyses publicly available judgements for evidentiary disparity and
selective prosecution patterns.
"""
import logging, os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL_PRIMARY", "llama-3.3-70b-versatile")


class JudgementQueryInput(BaseModel):
    judge_id: str = Field(description="Pseudonymous judge token")
    filters:  str = Field(description="JSON filter string: date_range, case_type")


class JudgementQueryTool(BaseTool):
    name = "query_judgements"
    description = "Query public judgement records for a judge"
    args_schema = JudgementQueryInput
    def _run(self, judge_id: str, filters: str) -> str:
        return f"[STUB] Judgements for {judge_id}: no live data in stub mode"


def run_judicial_analysis(event_data: dict) -> dict:
    """
    IMPORTANT: This agent analyses PUBLICLY AVAILABLE judgements only.
    It does not accuse judges of corruption. It surfaces statistical patterns
    in evidentiary standards and outcome distributions for human review.
    """
    llm = {"model": f"groq/{GROQ_MODEL}", "api_key": GROQ_API_KEY}
    analyst = Agent(
        role="Judicial Pattern Analyst",
        goal=(
            "Identify statistically significant patterns in publicly available court records. "
            "Focus on evidentiary basis (newspaper cuttings vs admissible evidence), "
            "outcome distributions by defendant category, and procedural anomalies. "
            "NEVER accuse. Surface patterns for human review only."
        ),
        backstory="Expert in legal analysis with focus on procedural fairness and evidentiary standards.",
        tools=[JudgementQueryTool()],
        llm=llm, verbose=False, max_iter=5,
    )
    task = Task(
        description=f"Analyse judicial patterns in: {str(event_data)[:1000]}. "
                    "Identify: evidentiary basis quality, outcome patterns, procedural anomalies.",
        expected_output="Pattern analysis: evidentiary score, outcome distribution, anomaly flag.",
        agent=analyst,
    )
    result = Crew(agents=[analyst], tasks=[task], process=Process.sequential, verbose=False).kickoff()
    return {"agent": "judicial_agent", "category": "C", "output": str(result)}
