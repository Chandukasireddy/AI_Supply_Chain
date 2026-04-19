from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    # Anthropic
    anthropic_api_key: str = Field(..., env="ANTHROPIC_API_KEY")

    # Google Gemini
    gemini_api_key: Optional[str] = None

    # Databricks — only needed when USE_MOCK_DB=false
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_cluster_id: Optional[str] = None
    databricks_warehouse_id: Optional[str] = None
    databricks_catalog: str = "db_supply_chain_workspace"
    databricks_schema: str = "supply_chain_data"

    # App
    use_mock_db: bool = True
    mock_db_path: str = "mock_data/db/supply_chain.duckdb"

    # LangGraph
    langgraph_checkpoint_dir: str = ".checkpoints"

    model_config = {"env_file": ".env", "case_sensitive": False}


settings = Settings()
