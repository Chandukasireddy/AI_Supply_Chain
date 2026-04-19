"""Demand specialist: forecasting and trend analysis."""
from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.databricks.client import DataClient
from src.databricks.queries import get_demand_forecast
from src.graph.state import AgentState

_SYSTEM = """\
You are a demand-planning analyst. Interpret demand forecast data and highlight
products with high/low predicted demand and high/low forecast confidence.
Provide actionable recommendations (e.g. pre-order, reduce safety stock).
"""

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM),
    ("human",
     "Task: {task}\n\n"
     "--- 30-day demand forecast ---\n{forecast}\n\n"
     "Summarise findings in 3-5 bullet points."),
])


def make_demand_node(client: DataClient):
    llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0, max_tokens=512)
    chain = _PROMPT | llm

    def demand_node(state: AgentState) -> dict:
        forecast = get_demand_forecast(client, days_ahead=30).to_string(index=False)

        response = chain.invoke({
            "task": state["task"],
            "forecast": forecast or "no forecast data available",
        })

        return {
            "messages": [HumanMessage(content=response.content, name="demand_agent")],
            "results": {"demand": response.content},
        }

    return demand_node
