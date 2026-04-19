"""
Unified data client.
  USE_MOCK_DB=true  → DuckDB (local file, mirrors Databricks SQL dialect)
  USE_MOCK_DB=false → databricks-connect Spark session
"""
from __future__ import annotations

import pandas as pd


class DataClient:
    def __init__(self, use_mock: bool = True, db_path: str = "mock_data/db/supply_chain.duckdb"):
        self.use_mock = use_mock
        if use_mock:
            import duckdb
            self._conn = duckdb.connect(db_path)
        else:
            self._spark = self._init_databricks()

    def _init_databricks(self):
        from databricks.connect import DatabricksSession
        from src.config import settings

        return (
            DatabricksSession.builder
            .host(settings.databricks_host)
            .token(settings.databricks_token)
            .clusterId(settings.databricks_cluster_id)
            .getOrCreate()
        )

    def query(self, sql: str) -> pd.DataFrame:
        if self.use_mock:
            return self._conn.execute(sql).df()
        return self._spark.sql(sql).toPandas()

    def close(self) -> None:
        if self.use_mock and hasattr(self, "_conn"):
            self._conn.close()

    def __enter__(self) -> "DataClient":
        return self

    def __exit__(self, *_) -> None:
        self.close()


def get_client() -> DataClient:
    from src.config import settings
    return DataClient(use_mock=settings.use_mock_db, db_path=settings.mock_db_path)
