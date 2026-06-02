"""
ARCAS - src/arcas_agents/enrichment/enrichment_agent.py
Short-cycle ReAct agent for entity enrichment from public sources.
Autonomous at confidence >= 0.9. Below threshold: proposes HITL merge.
"""
import logging, os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain.agents import create_react_agent, AgentExecutor
from langchain.tools import Tool
from langchain import hub

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL_FALLBACK", "llama-3.1-8b-instant")  # Use fast model


def enrich_entity(entity_id: str, surface_form: str, actor_type: str) -> dict:
    """
    Attempt to enrich an entity with additional public data.
    Returns enrichment result and confidence score.
    """
    llm = ChatGroq(model=GROQ_MODEL, api_key=GROQ_API_KEY, max_tokens=300)

    tools = [
        Tool(
            name="web_search_public",
            func=lambda q: f"[STUB] Web search for '{q}': no live data",
            description="Search public web sources for entity information",
        ),
    ]

    prompt = hub.pull("hwchase17/react")
    agent  = create_react_agent(llm, tools, prompt)
    executor = AgentExecutor(agent=agent, tools=tools, max_iterations=3, verbose=False)

    try:
        result = executor.invoke({
            "input": f"Find public information about: '{surface_form}' (type: {actor_type}). "
                     "Use only public sources. Return: full name, role, organisation, jurisdiction."
        })
        output     = result.get("output", "")
        confidence = 0.5  # Stub: real implementation uses similarity scoring
        return {"entity_id": entity_id, "enrichment": output, "confidence": confidence, "requires_hitl": confidence < 0.9}
    except Exception as e:
        log.warning(f"Enrichment failed for {entity_id}: {e}")
        return {"entity_id": entity_id, "enrichment": None, "confidence": 0.0, "requires_hitl": True}
