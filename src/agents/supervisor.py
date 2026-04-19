"""
Supervisor agent: uses Claude to decide which specialist to call next.
Returns updated `next_agent` and increments iteration counter.
"""
from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate

from src.graph.state import AgentState

MAX_ITERATIONS = 6

_SYSTEM = """\
You are the supervisor of a supply-chain intelligence system.
Available specialist agents:
  • inventory_agent  – stock levels, warehouse queries, reorder alerts, pending orders
  • demand_agent     – demand forecasting, trend analysis
  • supplier_agent   – supplier reliability, performance metrics

Rules:
1. Route to the most relevant agent for the user's task.
2. Once sufficient results have been gathered, respond with FINISH.
3. Never route to the same agent twice in a row.
4. Reply with ONLY one word: the agent name or FINISH.
"""

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM),
    ("human",
     "Task: {task}\n\n"
     "Results so far:\n{results}\n\n"
     "Iteration {iteration}/{max_iter}. Which agent next?"),
])

_llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0, max_tokens=16)


def supervisor_node(state: AgentState) -> dict:
    iteration = state.get("iteration", 0) + 1

    if iteration > MAX_ITERATIONS:
        return {"next_agent": "FINISH", "iteration": iteration}

    results_text = "\n".join(
        f"[{k}]\n{v}" for k, v in state.get("results", {}).items()
    ) or "none yet"

    chain = _PROMPT | _llm
    response = chain.invoke({
        "task": state["task"],
        "results": results_text,
        "iteration": iteration,
        "max_iter": MAX_ITERATIONS,
    })

    next_agent = response.content.strip()
    valid = {"inventory_agent", "demand_agent", "supplier_agent", "FINISH"}
    if next_agent not in valid:
        next_agent = "FINISH"

    return {"next_agent": next_agent, "iteration": iteration}


def route_after_supervisor(state: AgentState) -> str:
    """Conditional edge function — maps next_agent to a graph node name."""
    agent = state.get("next_agent", "FINISH")
    return "__end__" if agent == "FINISH" else agent
