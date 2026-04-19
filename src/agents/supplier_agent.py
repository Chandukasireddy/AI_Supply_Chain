"""Supplier specialist: reliability scoring and order tracking."""
from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.databricks.client import DataClient
from src.databricks.queries import get_supplier_performance
from src.graph.state import AgentState

_SYSTEM = """\
You are a supplier-relations analyst. Evaluate supplier performance data and
flag suppliers with low reliability scores or abnormally long delivery times.
Recommend whether to diversify sourcing or place larger orders with top performers.
"""

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM),
    ("human",
     "Task: {task}\n\n"
     "--- Supplier performance ---\n{perf}\n\n"
     "Summarise findings in 3-5 bullet points."),
])


def make_supplier_node(client: DataClient):
    llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0, max_tokens=512)
    chain = _PROMPT | llm

    def supplier_node(state: AgentState) -> dict:
        perf = get_supplier_performance(client).to_string(index=False)

        response = chain.invoke({
            "task": state["task"],
            "perf": perf or "no supplier data available",
        })

        return {
            "messages": [HumanMessage(content=response.content, name="supplier_agent")],
            "results": {"supplier": response.content},
        }

    return supplier_node
