"""Entry point — initialises DB if needed, then runs a sample query through the graph."""
from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

load_dotenv()


def _ensure_db():
    db_path = os.environ.get("MOCK_DB_PATH", "mock_data/db/supply_chain.duckdb")
    if not pathlib.Path(db_path).exists():
        print("Initialising mock database …")
        from mock_data.init_db import init
        init()


def run(task: str) -> str:
    from src.config import settings
    from src.databricks.client import DataClient
    from src.graph.workflow import build_graph

    with DataClient(use_mock=settings.use_mock_db, db_path=settings.mock_db_path) as client:
        graph = build_graph(client)

        initial_state = {
            "messages":   [],
            "task":       task,
            "next_agent": "",
            "results":    {},
            "iteration":  0,
        }

        final_state = graph.invoke(initial_state)

    results = final_state.get("results", {})
    return "\n\n".join(f"### {k}\n{v}" for k, v in results.items())


if __name__ == "__main__":
    _ensure_db()

    sample_task = (
        "Give me a full supply-chain health report: "
        "highlight low-stock items, upcoming demand spikes, "
        "and any underperforming suppliers."
    )

    print("Running supply-chain multi-agent …\n")
    report = run(sample_task)
    print(report)
