"""Builds and compiles the LangGraph StateGraph."""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from src.agents.demand_agent import make_demand_node
from src.agents.inventory_agent import make_inventory_node
from src.agents.supervisor import route_after_supervisor, supervisor_node
from src.agents.supplier_agent import make_supplier_node
from src.databricks.client import DataClient
from src.graph.state import AgentState


def build_graph(client: DataClient):
    graph = StateGraph(AgentState)

    # ── Nodes ──────────────────────────────────────────────────────────────
    graph.add_node("supervisor",      supervisor_node)
    graph.add_node("inventory_agent", make_inventory_node(client))
    graph.add_node("demand_agent",    make_demand_node(client))
    graph.add_node("supplier_agent",  make_supplier_node(client))

    # ── Entry ──────────────────────────────────────────────────────────────
    graph.set_entry_point("supervisor")

    # ── Supervisor → specialist (conditional) ─────────────────────────────
    graph.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {
            "inventory_agent": "inventory_agent",
            "demand_agent":    "demand_agent",
            "supplier_agent":  "supplier_agent",
            "__end__":         END,
        },
    )

    # ── Specialists always return to supervisor ────────────────────────────
    graph.add_edge("inventory_agent", "supervisor")
    graph.add_edge("demand_agent",    "supervisor")
    graph.add_edge("supplier_agent",  "supervisor")

    return graph.compile()
