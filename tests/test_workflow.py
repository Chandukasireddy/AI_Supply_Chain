"""Basic integration tests — use the in-memory DuckDB mock."""
import os
import tempfile

import pytest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ["USE_MOCK_DB"] = "true"


@pytest.fixture()
def db_client(tmp_path):
    from mock_data.init_db import init
    db_path = str(tmp_path / "test.duckdb")
    os.environ["MOCK_DB_PATH"] = db_path

    import importlib, mock_data.init_db as m
    orig = os.environ.get("MOCK_DB_PATH")
    os.environ["MOCK_DB_PATH"] = db_path
    m.DB_PATH = db_path
    m.init()

    from src.databricks.client import DataClient
    client = DataClient(use_mock=True, db_path=db_path)
    yield client
    client.close()


def test_low_stock_query(db_client):
    from src.databricks.queries import get_low_stock_items
    df = get_low_stock_items(db_client)
    assert df is not None
    assert "name" in df.columns


def test_demand_forecast_query(db_client):
    from src.databricks.queries import get_demand_forecast
    df = get_demand_forecast(db_client, days_ahead=30)
    assert df is not None
    assert "predicted_demand" in df.columns


def test_supplier_performance_query(db_client):
    from src.databricks.queries import get_supplier_performance
    df = get_supplier_performance(db_client)
    assert len(df) > 0
    assert "reliability_score" in df.columns


def test_graph_builds(db_client):
    from src.graph.workflow import build_graph
    graph = build_graph(db_client)
    assert graph is not None
