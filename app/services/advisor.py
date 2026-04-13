from __future__ import annotations

from typing import Any

from app.models import (
    BuilderExposure,
    ExecutionAnalysis,
    Explanation,
    HarmProxy,
    LossEstimate,
    PairRisk,
    RiskLevel,
    RouteRecommendation,
    WalletAnalysis,
)
from app.services.dune_queries import DuneQueryRunner, lookback_window
from app.services.risk import risk_level, wallet_risk_score


class AdvisorService:
    def __init__(self, runner: DuneQueryRunner | None = None) -> None:
        self.runner = runner or DuneQueryRunner()

    def analyze_wallet(self, wallet: str, lookback_days: int = 30) -> WalletAnalysis:
        start_time, end_time = lookback_window(lookback_days)
        rows = self.runner.execute_registered_query(
            "wallet_mev_exposure",
            {"wallet": wallet, "start_time": start_time, "end_time": end_time},
        )
        row = rows[0] if rows else {}
        total_dex_txs = int(row.get("total_dex_txs") or 0)
        sandwiched_txs = int(row.get("sandwiched_txs") or 0)
        sandwich_txs = int(row.get("sandwich_txs") or 0)
        sandwiched_volume_usd = float(row.get("sandwiched_volume_usd") or 0)
        total_volume_usd = float(row.get("total_volume_usd") or 0)
        high_risk_pair_share = float(row.get("high_risk_pair_share") or 0)
        builder_concentration = float(row.get("builder_concentration") or 0)
        risk = wallet_risk_score(
            total_dex_txs=total_dex_txs,
            sandwiched_txs=sandwiched_txs,
            sandwiched_volume_usd=sandwiched_volume_usd,
            total_volume_usd=total_volume_usd,
            high_risk_pair_share=high_risk_pair_share,
            builder_concentration=builder_concentration,
        )
        return WalletAnalysis(
            wallet=wallet,
            lookback_days=lookback_days,
            risk=risk,
            total_dex_txs=total_dex_txs,
            sandwiched_txs=sandwiched_txs,
            sandwich_txs=sandwich_txs,
            sandwiched_volume_usd=sandwiched_volume_usd,
            estimated_loss=LossEstimate(amount_usd=sandwiched_volume_usd),
            harm_proxy=HarmProxy(
                affected_notional_usd=sandwiched_volume_usd,
                confidence=0.45 if sandwiched_txs else 0.2,
            ),
            confidence=0.45 if total_dex_txs else 0.2,
            signal_source="dune_curated_sandwich_tables_with_inferred_builder_context",
            top_pairs=coerce_list(row.get("top_pairs")),
            top_tokens=coerce_list(row.get("top_tokens")),
            evidence=rows[:20],
        )

    def analyze_execution(self, tx_hash: str) -> ExecutionAnalysis:
        start_time, end_time = lookback_window(30)
        rows = self.runner.execute_registered_query(
            "tx_execution_context",
            {"tx_hash": tx_hash, "start_time": start_time, "end_time": end_time},
        )
        row = rows[0] if rows else {}
        classification = str(row.get("classification") or "unknown")
        amount_usd = float(row.get("amount_usd") or 0)
        loss_amount = amount_usd if classification == "sandwiched" else 0.0
        confidence = classification_confidence(classification)
        return ExecutionAnalysis(
            tx_hash=tx_hash,
            classification=classification,
            block_number=maybe_int(row.get("block_number")),
            block_time=row.get("block_time"),
            builder_brand=row.get("builder_brand"),
            validator_address=row.get("validator_address"),
            validator_confidence=str(row.get("validator_confidence") or "unknown"),
            amount_usd=amount_usd,
            estimated_loss=LossEstimate(amount_usd=loss_amount),
            harm_proxy=HarmProxy(
                affected_notional_usd=loss_amount,
                confidence=confidence,
            ),
            confidence=confidence,
            signal_source=classification_signal_source(classification),
            evidence=rows[:20],
        )

    def explain_execution(self, tx_hash: str) -> Explanation:
        analysis = self.analyze_execution(tx_hash)
        level = execution_risk_level(analysis.classification)
        builder = analysis.builder_brand or "unknown builder"
        if analysis.classification == "sandwiched":
            summary = (
                f"Transaction {tx_hash} appears sandwiched on BNB. "
                f"The conservative affected volume estimate is "
                f"${analysis.estimated_loss.amount_usd:,.2f}, with block context attributed "
                f"to {builder}."
            )
        elif analysis.classification == "sandwich":
            summary = (
                f"Transaction {tx_hash} appears to be part of a sandwich sequence, "
                f"not a protected user execution. Builder context: {builder}."
            )
        else:
            summary = (
                f"Transaction {tx_hash} is not classified as sandwiched in the current "
                "Dune baseline. "
                f"Builder context: {builder}."
            )
        return Explanation(
            tx_hash=tx_hash,
            summary=summary,
            risk_level=level,
            evidence=analysis.evidence,
        )

    def get_builder_mev_exposure(self, lookback_days: int = 7) -> BuilderExposure:
        start_time, end_time = lookback_window(lookback_days)
        rows = self.runner.execute_registered_query(
            "builder_sandwich_exposure",
            {"start_time": start_time, "end_time": end_time},
        )
        return BuilderExposure(lookback_days=lookback_days, rows=rows)

    def get_pair_risk(self, token_pair_or_addresses: str, lookback_days: int = 30) -> PairRisk:
        start_time, end_time = lookback_window(lookback_days)
        rows = self.runner.execute_registered_query(
            "pair_token_risk",
            {
                "start_time": start_time,
                "end_time": end_time,
                "token_pair_or_addresses": token_pair_or_addresses,
            },
        )
        return PairRisk(
            token_pair_or_addresses=token_pair_or_addresses,
            lookback_days=lookback_days,
            rows=rows,
        )

    def recommend_route(
        self,
        pair: str,
        amount: float,
        priority: str = "safe",
    ) -> RouteRecommendation:
        pair_risk = self.get_pair_risk(pair, lookback_days=30)
        worst_ratio = max(
            (float(row.get("sandwiched_transactions_percentage") or 0) for row in pair_risk.rows),
            default=0.0,
        )
        score = round(worst_ratio * 100)
        level = risk_level(score)
        if level in {RiskLevel.high, RiskLevel.critical}:
            recommendation = "Use a lower-exposure execution path candidate or delay execution."
        elif level == RiskLevel.medium:
            recommendation = (
                "Prefer private/low-latency path candidates and reduce slippage tolerance."
            )
        else:
            recommendation = (
                "Historical pair risk is low; standard execution path candidates are acceptable."
            )
        rationale = (
            "This is an advisory based on historical Dune observations, "
            "not a live routing guarantee. "
            "RPC provider attribution is not directly observable on-chain."
        )
        return RouteRecommendation(
            pair=pair,
            amount=amount,
            priority=priority,
            recommendation=recommendation,
            risk_level=level,
            rationale=rationale,
            confidence=0.3,
            signal_source="historical_pair_sandwiched_share_inferred_route_advisory",
            harm_proxy=HarmProxy(
                affected_notional_usd=0,
                method="route_advisory_risk_no_live_counterfactual",
                confidence=0.3,
            ),
            evidence=pair_risk.rows[:20],
        )


def coerce_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def execution_risk_level(classification: str) -> RiskLevel:
    if classification == "sandwiched":
        return RiskLevel.high
    if classification == "sandwich":
        return RiskLevel.medium
    return RiskLevel.low


def classification_confidence(classification: str) -> float:
    if classification in {"sandwiched", "sandwich"}:
        return 0.75
    if classification == "other":
        return 0.45
    return 0.15


def classification_signal_source(classification: str) -> str:
    if classification in {"sandwiched", "sandwich"}:
        return "dune_curated_sandwich_tables_observed"
    if classification == "other":
        return "dune_dex_trade_without_curated_sandwich_label"
    return "no_matching_dune_trade_row"
