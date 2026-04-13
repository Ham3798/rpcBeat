from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import httpx

TERMINAL_STATES = {"QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}


class DuneError(RuntimeError):
    def __init__(self, message: str, *, response: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.response = response or {}


@dataclass(frozen=True)
class DuneParameter:
    name: str
    value: Any
    type: str = "text"

    def as_create_payload(self) -> dict[str, Any]:
        return {"key": self.name, "type": self.type, "value": self.value}

    def as_execute_value(self) -> Any:
        return self.value

    def as_payload(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "value": self.value}


class DuneClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.dune.com/api/v1",
        timeout_seconds: float = 60.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._owns_client = client is None
        self._headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> DuneClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def create_query(
        self,
        *,
        name: str,
        query_sql: str,
        description: str = "",
        tags: list[str] | None = None,
        is_private: bool = True,
        parameters: Iterable[DuneParameter] | dict[str, Any] | None = None,
    ) -> int:
        payload = {
            "name": name,
            "query_sql": query_sql,
            "description": description,
            "tags": tags or ["rpcbeat", "bnb", "mev"],
            "is_private": is_private,
        }
        query_parameters = normalize_parameters(parameters)
        if query_parameters:
            payload["parameters"] = [param.as_create_payload() for param in query_parameters]
        data = self._request("POST", "/query", json=payload)
        query_id = data.get("query_id") or data.get("id")
        if query_id is None:
            raise DuneError("Dune create query response did not include query_id", response=data)
        return int(query_id)

    def update_query(
        self,
        query_id: int,
        *,
        name: str,
        query_sql: str,
        description: str = "",
        tags: list[str] | None = None,
        parameters: Iterable[DuneParameter] | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "name": name,
            "query_sql": query_sql,
            "description": description,
            "tags": tags or ["rpcbeat", "bnb", "mev"],
        }
        query_parameters = normalize_parameters(parameters)
        if query_parameters:
            payload["parameters"] = [param.as_create_payload() for param in query_parameters]
        return self._request("PATCH", f"/query/{query_id}", json=payload)

    def execute_query(
        self,
        query_id: int,
        *,
        parameters: Iterable[DuneParameter] | dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = {}
        query_parameters = normalize_parameters(parameters)
        if query_parameters:
            payload["query_parameters"] = {
                param.name: param.as_execute_value() for param in query_parameters
            }
        data = self._request("POST", f"/query/{query_id}/execute", json=payload)
        execution_id = data.get("execution_id")
        if not execution_id:
            raise DuneError(
                "Dune execute query response did not include execution_id",
                response=data,
            )
        return str(execution_id)

    def get_execution_status(self, execution_id: str) -> dict[str, Any]:
        return self._request("GET", f"/execution/{execution_id}/status")

    def get_execution_result(
        self,
        execution_id: str,
        *,
        limit: int = 1000,
        offset: int = 0,
        allow_partial_results: bool = False,
    ) -> dict[str, Any]:
        params = {
            "limit": limit,
            "offset": offset,
            "allow_partial_results": str(allow_partial_results).lower(),
        }
        return self._request("GET", f"/execution/{execution_id}/results", params=params)

    def wait_for_execution(
        self,
        execution_id: str,
        *,
        poll_interval_seconds: float = 2.0,
        timeout_seconds: float = 300.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last_status = self.get_execution_status(execution_id)
            state = str(last_status.get("state") or last_status.get("execution_state") or "")
            if state in TERMINAL_STATES:
                if state != "QUERY_STATE_COMPLETED":
                    raise DuneError(f"Dune execution ended in {state}", response=last_status)
                return last_status
            time.sleep(poll_interval_seconds)
        raise DuneError("Timed out waiting for Dune execution", response=last_status)

    def fetch_all_rows(
        self,
        execution_id: str,
        *,
        max_rows: int = 5000,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while len(rows) < max_rows:
            result = self.get_execution_result(execution_id, limit=page_size, offset=offset)
            page_rows = extract_rows(result)
            if not page_rows:
                break
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += page_size
        return rows[:max_rows]

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._client.request(
            method,
            f"{self.base_url}{path}",
            headers=self._headers,
            **kwargs,
        )
        try:
            data = response.json()
        except ValueError:
            data = {"text": response.text}
        if response.status_code >= 400:
            raise DuneError(f"Dune API returned HTTP {response.status_code}", response=data)
        if not isinstance(data, dict):
            raise DuneError("Dune API returned a non-object response", response={"data": data})
        return data


def normalize_parameters(
    parameters: Iterable[DuneParameter] | dict[str, Any] | None,
) -> list[DuneParameter]:
    if parameters is None:
        return []
    if isinstance(parameters, dict):
        return [DuneParameter(name=name, value=value) for name, value in parameters.items()]
    normalized: list[DuneParameter] = []
    for parameter in parameters:
        if isinstance(parameter, DuneParameter):
            normalized.append(parameter)
        elif isinstance(parameter, dict):
            name = parameter.get("name") or parameter.get("key")
            if not name:
                raise DuneError("Dune parameter is missing `name` or `key`.", response=parameter)
            normalized.append(
                DuneParameter(
                    name=str(name),
                    value=parameter.get("value"),
                    type=str(parameter.get("type") or "text"),
                )
            )
        else:
            raise DuneError("Unsupported Dune parameter shape.", response={"parameter": parameter})
    return normalized


def extract_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    if "result" in result and isinstance(result["result"], dict):
        rows = result["result"].get("rows")
        if isinstance(rows, list):
            return rows
    rows = result.get("rows")
    if isinstance(rows, list):
        return rows
    return []
