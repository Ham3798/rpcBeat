from __future__ import annotations

from typing import Any

from app.models import RiskLevel
from app.services.advisor import AdvisorService
from app.services.dune_queries import QueryUnavailable


class FakeRunner:
    def execute_registered_query(self, key: str, parameters: dict[str, Any], *, max_rows=None):
        if key == "wallet_mev_exposure":
            return [
                {
                    "total_dex_txs": 10,
                    "sandwiched_txs": 2,
                    "sandwich_txs": 1,
                    "total_volume_usd": 1000,
                    "sandwiched_volume_usd": 100,
                    "high_risk_pair_share": 0.5,
                    "builder_concentration": 0.2,
                }
            ]
        if key == "wallet_builder_context":
            return [
                {
                    "builder_brand": "ExampleBuilder",
                    "wallet_blocks": 4,
                    "wallet_txs": 10,
                    "wallet_volume_usd": 1000,
                    "wallet_sandwiched_txs": 2,
                    "wallet_sandwiched_volume_usd": 100,
                    "wallet_block_share": 0.8,
                    "affected_block_share": 0.5,
                    "attribution_confidence": "attributed",
                }
            ]
        if key == "wallet_pool_context":
            return [
                {
                    "project": "ExampleDex",
                    "project_contract_address": "0xpool",
                    "token_pair": "WBNB-USDT",
                    "wallet_txs": 10,
                    "wallet_volume_usd": 1000,
                    "wallet_sandwiched_txs": 2,
                    "wallet_sandwiched_volume_usd": 100,
                    "pool_sandwiched_transactions_percentage": 0.3,
                    "pool_sandwiched_volume_percentage": 0.2,
                }
            ]
        if key == "tx_execution_context":
            return [
                {
                    "classification": "sandwiched",
                    "block_number": 1,
                    "block_time": "2026-04-13T00:00:00Z",
                    "route_class": "dex_router_or_aggregator",
                    "builder_brand": "ExampleBuilder",
                    "validator_address": "0xvalidator",
                    "validator_role_label": "block_miner_or_proposer",
                    "validator_attribution_basis": "bnb.blocks.miner",
                    "validator_confidence": "attributed",
                    "amount_usd": 42,
                }
            ]
        if key == "pair_token_risk":
            return [{"token_pair": "WBNB-USDT", "sandwiched_transactions_percentage": 0.7}]
        if key == "builder_sandwich_exposure":
            return [
                {
                    "hour": "2026-04-13T00:00:00Z",
                    "builder_brand": "ExampleBuilder",
                    "sandwich_tx_count": 2,
                    "sandwiched_tx_count": 1,
                }
            ]
        return []


class PartialFailureRunner(FakeRunner):
    def execute_registered_query(self, key: str, parameters: dict[str, Any], *, max_rows=None):
        if key in {"wallet_builder_context", "wallet_pool_context"}:
            raise QueryUnavailable(f"{key} unavailable")
        return super().execute_registered_query(key, parameters, max_rows=max_rows)


def test_analyze_wallet_returns_metric_packet_without_score() -> None:
    result = AdvisorService(FakeRunner()).analyze_wallet("0xabc", lookback_days=7)
    payload = result.model_dump()

    assert result.wallet == "0xabc"
    assert "risk" not in payload
    assert result.wallet_orderflow["total_dex_txs"] == 10
    assert result.wallet_mev_harm["sandwiched_txs"] == 2
    assert result.wallet_mev_harm["harm_proxy"]["affected_notional_usd"] == 100
    assert result.builder_context["available"] is True
    assert result.pool_context["wallet_pool_exposure"][0]["token_pair"] == "WBNB-USDT"
    assert result.validator_context["available"] is False
    assert result.validator_context["validator_attribution_basis"] == "bnb.blocks.miner"
    assert result.rpc_path_inference["rpc_provider_observed"] is False
    assert "dune_curated" in result.signal_source


def test_analyze_wallet_keeps_partial_context_when_optional_queries_fail() -> None:
    result = AdvisorService(PartialFailureRunner()).analyze_wallet("0xabc", lookback_days=7)

    assert result.wallet_orderflow["total_dex_txs"] == 10
    assert result.builder_context["available"] is False
    assert result.builder_context["confidence"] == "unknown"
    assert "wallet_builder_context unavailable" in result.builder_context["error"]
    assert result.pool_context["available"] is False
    assert "wallet_pool_context unavailable" in result.pool_context["error"]


def test_analyze_execution_includes_validator_block_context() -> None:
    result = AdvisorService(FakeRunner()).analyze_execution("0xtx")

    assert result.route_class == "dex_router_or_aggregator"
    assert result.validator_address == "0xvalidator"
    assert result.validator_role_label == "block_miner_or_proposer"
    assert result.validator_attribution_basis == "bnb.blocks.miner"
    assert result.validator_confidence == "attributed"


def test_explain_execution_uses_classification() -> None:
    result = AdvisorService(FakeRunner()).explain_execution("0xtx")

    assert result.risk_level == RiskLevel.high
    assert "appears sandwiched" in result.summary


def test_recommend_route_is_advisory() -> None:
    result = AdvisorService(FakeRunner()).recommend_route("WBNB-USDT", 1000)

    assert "execution-path context" in result.advisory
    assert result.pair_context
    assert result.builder_context
    assert "RPC provider attribution is not directly observable on-chain." in result.limitations
    assert result.signal_source == "historical_pair_and_builder_exposure_inferred_route_advisory"
