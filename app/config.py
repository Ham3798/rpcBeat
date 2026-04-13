from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    dune_api_key: str | None = Field(default=None, alias="DUNE_API_KEY")
    dune_base_url: str = Field(default="https://api.dune.com/api/v1", alias="DUNE_BASE_URL")
    rpcbeat_dune_private: bool = Field(default=True, alias="RPCBEAT_DUNE_PRIVATE")
    rpcbeat_default_lookback_days: int = Field(default=30, alias="RPCBEAT_DEFAULT_LOOKBACK_DAYS")
    rpcbeat_max_result_rows: int = Field(default=5000, alias="RPCBEAT_MAX_RESULT_ROWS")
    rpcbeat_query_dir: Path = Field(default=Path("queries"), alias="RPCBEAT_QUERY_DIR")
    rpcbeat_query_registry: Path = Field(
        default=Path("queries/registry.json"), alias="RPCBEAT_QUERY_REGISTRY"
    )
    rpcbeat_eval_dir: Path = Field(default=Path("evals"), alias="RPCBEAT_EVAL_DIR")
    rpcbeat_dune_poll_interval_seconds: float = Field(
        default=2.0, alias="RPCBEAT_DUNE_POLL_INTERVAL_SECONDS"
    )
    rpcbeat_dune_timeout_seconds: float = Field(default=60.0, alias="RPCBEAT_DUNE_TIMEOUT_SECONDS")


@lru_cache
def get_settings() -> Settings:
    return Settings()

