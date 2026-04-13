from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import Settings, get_settings
from app.dune.client import DuneClient, DuneError
from app.dune.registry import QueryRegistry


class QueryUnavailable(RuntimeError):
    pass


class DuneQueryRunner:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.registry = QueryRegistry(
            self.settings.rpcbeat_query_dir,
            self.settings.rpcbeat_query_registry,
        )

    def execute_registered_query(
        self,
        key: str,
        parameters: dict[str, Any],
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        if not self.settings.dune_api_key:
            raise QueryUnavailable("DUNE_API_KEY is not configured.")
        query_id = self.registry.get_query_id(key)
        if query_id is None:
            raise QueryUnavailable(
                f"Query `{key}` is not synced. Run `rpcbeat query sync` after setting DUNE_API_KEY."
            )
        with DuneClient(
            self.settings.dune_api_key,
            base_url=self.settings.dune_base_url,
            timeout_seconds=self.settings.rpcbeat_dune_timeout_seconds,
        ) as client:
            try:
                execution_id = client.execute_query(query_id, parameters=parameters)
                client.wait_for_execution(
                    execution_id,
                    poll_interval_seconds=self.settings.rpcbeat_dune_poll_interval_seconds,
                    timeout_seconds=self.settings.rpcbeat_dune_timeout_seconds,
                )
                return client.fetch_all_rows(
                    execution_id,
                    max_rows=max_rows or self.settings.rpcbeat_max_result_rows,
                )
            except DuneError as exc:
                raise QueryUnavailable(str(exc)) from exc


def lookback_window(lookback_days: int) -> tuple[str, str]:
    end = datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(days=lookback_days)
    return start.isoformat(), end.isoformat()

