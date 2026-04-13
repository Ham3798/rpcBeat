from __future__ import annotations

from typing import Any

from app.models import RiskLevel
from app.services.advisor import AdvisorService


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
        if key == "tx_execution_context":
            return [
                {
                    "classification": "sandwiched",
                    "block_number": 1,
                    "block_time": "2026-04-13T00:00:00Z",
                    "builder_brand": "ExampleBuilder",
                    "validator_address": None,
                    "validator_confidence": "unknown",
                    "amount_usd": 42,
                }
            ]
        if key == "pair_token_risk":
            return [{"token_pair": "WBNB-USDT", "sandwiched_transactions_percentage": 0.7}]
        if key == "builder_sandwich_exposure":
            return [{"hour": "2026-04-13T00:00:00Z", "builder_brand": "ExampleBuilder"}]
        return []


def test_analyze_wallet_maps_rows_to_score() -> None:
    result = AdvisorService(FakeRunner()).analyze_wallet("0xabc", lookback_days=7)

    assert result.wallet == "0xabc"
    assert result.total_dex_txs == 10
    assert result.risk.score == 24
    assert result.risk.level == RiskLevel.low
    assert result.harm_proxy.affected_notional_usd == 100
    assert "dune_curated" in result.signal_source


def test_explain_execution_uses_classification() -> None:
    result = AdvisorService(FakeRunner()).explain_execution("0xtx")

    assert result.risk_level == RiskLevel.high
    assert "appears sandwiched" in result.summary


def test_recommend_route_is_advisory() -> None:
    result = AdvisorService(FakeRunner()).recommend_route("WBNB-USDT", 1000)

    assert result.risk_level == RiskLevel.high
    assert "advisory" in result.rationale
    assert result.signal_source == "historical_pair_sandwiched_share_inferred_route_advisory"
