from __future__ import annotations

import json

import httpx

from app.dune.client import DuneClient, extract_rows, normalize_parameters


def test_normalize_parameters_from_dict() -> None:
    params = normalize_parameters({"wallet": "0xabc", "lookback": 7})

    assert [param.as_payload() for param in params] == [
        {"name": "wallet", "type": "text", "value": "0xabc"},
        {"name": "lookback", "type": "text", "value": 7},
    ]


def test_normalize_parameters_from_metadata_shape() -> None:
    params = normalize_parameters([{"key": "wallet", "value": "0xabc", "type": "text"}])

    assert [param.as_create_payload() for param in params] == [
        {"key": "wallet", "type": "text", "value": "0xabc"}
    ]


def test_execute_query_request_body() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"execution_id": "exec_1"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    dune = DuneClient("test-key", client=client)

    execution_id = dune.execute_query(123, parameters={"wallet": "0xabc"})

    assert execution_id == "exec_1"
    assert requests[0].url.path == "/api/v1/query/123/execute"
    assert requests[0].headers["X-Dune-API-Key"] == "test-key"
    assert json.loads(requests[0].content) == {"query_parameters": {"wallet": "0xabc"}}


def test_create_query_includes_dune_parameter_shape() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"query_id": 123})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    dune = DuneClient("test-key", client=client)

    query_id = dune.create_query(
        name="test",
        query_sql="SELECT '{{wallet}}'",
        parameters={"wallet": "0xabc"},
    )

    assert query_id == 123
    assert json.loads(requests[0].content)["parameters"] == [
        {"key": "wallet", "type": "text", "value": "0xabc"}
    ]


def test_extract_rows_supports_nested_and_flat_shapes() -> None:
    assert extract_rows({"result": {"rows": [{"a": 1}]}}) == [{"a": 1}]
    assert extract_rows({"rows": [{"b": 2}]}) == [{"b": 2}]
    assert extract_rows({"result": {}}) == []
