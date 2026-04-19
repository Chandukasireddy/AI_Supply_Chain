"""Inventory specialist: low-stock alerts, warehouse levels, pending orders."""
from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.databricks.client import DataClient
from src.databricks.queries import (
    get_inventory_summary,
    get_low_stock_items,
    get_pending_orders,
)
from src.graph.state import AgentState

_SYSTEM = """\
You are an inventory analyst. You receive raw query results from a supply-chain
database and produce a concise, actionable summary for the supervisor.
Focus on items that need attention (low stock, overdue orders).
"""

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM),
    ("human", "Task: {task}\n\n--- Low-stock items ---\n{low_stock}\n\n"
              "--- Pending orders ---\n{pending}\n\n"
              "--- Inventory summary by category ---\n{summary}\n\n"
              "Summarise the key findings in 3-5 bullet points."),
])


def make_inventory_node(client: DataClient):
    llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0, max_tokens=512)
    chain = _PROMPT | llm

    def inventory_node(state: AgentState) -> dict:
        low_stock = get_low_stock_items(client).to_string(index=False)
        pending   = get_pending_orders(client).to_string(index=False)
        summary   = get_inventory_summary(client).to_string(index=False)

        response = chain.invoke({
            "task": state["task"],
            "low_stock": low_stock or "none",
            "pending":   pending   or "none",
            "summary":   summary   or "none",
        })

        return {
            "messages": [HumanMessage(content=response.content, name="inventory_agent")],
            "results": {"inventory": response.content},
        }

    return inventory_node
