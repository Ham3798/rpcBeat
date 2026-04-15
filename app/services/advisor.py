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
from app.services.dune_queries import (
    DuneQueryRunner,
    QueryUnavailable,
    lookback_window,
)


class AdvisorService:
    def __init__(self, runner: DuneQueryRunner | None = None) -> None:
        self.runner = runner or DuneQueryRunner()

    def analyze_wallet(self, wallet: str, lookback_days: int = 30) -> WalletAnalysis:
        start_time, end_time = lookback_window(lookback_days)
        rows = self.runner.execute_registered_query(
            "wallet_mev_exposure",
            {"wallet": wallet, "start_time": start_time, "end_time": end_time},
        )
        builder_rows, builder_error = self.execute_optional_wallet_query(
            "wallet_builder_context",
            {"wallet": wallet, "start_time": start_time, "end_time": end_time},
        )
        pool_rows, pool_error = self.execute_optional_wallet_query(
            "wallet_pool_context",
            {"wallet": wallet, "start_time": start_time, "end_time": end_time},
        )
        row = rows[0] if rows else {}
        total_dex_txs = int(row.get("total_dex_txs") or 0)
        sandwiched_txs = int(row.get("sandwiched_txs") or 0)
        sandwich_txs = int(row.get("sandwich_txs") or 0)
        sandwiched_volume_usd = float(row.get("sandwiched_volume_usd") or 0)
        total_volume_usd = float(row.get("total_volume_usd") or 0)
        return WalletAnalysis(
            wallet=wallet,
            lookback_days=lookback_days,
            wallet_orderflow={
                "total_dex_txs": total_dex_txs,
                "total_volume_usd": total_volume_usd,
                "top_projects": summarize_top(pool_rows, "project", "wallet_volume_usd"),
                "top_pairs": summarize_top(pool_rows, "token_pair", "wallet_volume_usd"),
                "top_pools": pool_rows[:10],
            },
            wallet_mev_harm={
                "sandwiched_txs": sandwiched_txs,
                "sandwiched_tx_ratio": safe_ratio(sandwiched_txs, total_dex_txs),
                "sandwich_actor_txs": sandwich_txs,
                "sandwiched_volume_usd": sandwiched_volume_usd,
                "sandwiched_volume_ratio": safe_ratio(sandwiched_volume_usd, total_volume_usd),
                "harm_proxy": HarmProxy(
                    affected_notional_usd=sandwiched_volume_usd,
                    confidence=0.45 if sandwiched_txs else 0.2,
                ).model_dump(mode="json"),
                "affected_tx_samples": rows[:20],
            },
            builder_context={
                "available": builder_error is None,
                "confidence": "attributed" if builder_error is None else "unknown",
                "wallet_builder_distribution": builder_rows,
                "affected_builder_distribution": [
                    item
                    for item in builder_rows
                    if float(item.get("affected_block_share") or 0) > 0
                ],
                "global_builder_baseline": [],
                "error": builder_error,
            },
            pool_context={
                "available": pool_error is None,
                "confidence": "observed" if pool_error is None else "unknown",
                "wallet_pool_exposure": pool_rows,
                "error": pool_error,
            },
            validator_context={
                "available": False,
                "confidence": "unknown",
                "validator_role_label": "block_miner_or_proposer",
                "validator_attribution_basis": "bnb.blocks.miner",
                "reason": "Wallet-level validator distribution is intentionally out of v1 core.",
                "limitations": [
                    (
                        "Validator/proposer context is exposed at tx/block level through "
                        "tx_execution_context."
                    ),
                    (
                        "It does not prove validator intent, causality, RPC provider, "
                        "or private relay path."
                    ),
                ],
            },
            rpc_path_inference={
                "rpc_provider_observed": False,
                "confidence": "inferred",
                "inference_basis": [
                    "wallet builder distribution",
                    "affected block clustering",
                    "pool-level sandwich pressure",
                    "wallet historical sandwich outcomes",
                ],
                "limitations": [
                    "RPC provider identity is not directly observable on-chain.",
                    "Private relay admission and builder-proxy latency are not visible.",
                    "The response is advisory context, not path ground truth.",
                ],
                "questions_for_llm": [
                    "Is this wallet concentrated in a small set of builder contexts?",
                    "Did the wallet experience observed sandwiched flow?",
                    "Are the wallet's main pools historically sandwich-prone?",
                    "Does the available evidence justify changing execution path candidates?",
                ],
            },
            confidence=0.45 if total_dex_txs else 0.2,
            signal_source="dune_curated_sandwich_tables_with_inferred_builder_context",
            evidence=[
                {"query_key": "wallet_mev_exposure", "rows": rows[:20]},
                {
                    "query_key": "wallet_builder_context",
                    "rows": builder_rows[:20],
                    "error": builder_error,
                },
                {"query_key": "wallet_pool_context", "rows": pool_rows[:20], "error": pool_error},
            ],
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
            route_class=row.get("route_class"),
            builder_brand=row.get("builder_brand"),
            validator_address=row.get("validator_address"),
            validator_role_label=row.get("validator_role_label"),
            validator_attribution_basis=row.get("validator_attribution_basis"),
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
        builder_exposure = self.get_builder_mev_exposure(lookback_days=7)
        worst_pair_ratio = max(
            (
                float(row.get("sandwiched_transactions_percentage") or 0)
                for row in pair_risk.rows
            ),
            default=0.0,
        )
        high_builder_rows = [
            row
            for row in builder_exposure.rows
            if int(row.get("sandwich_tx_count") or 0)
            + int(row.get("sandwiched_tx_count") or 0)
            > 0
        ]
        advisory = (
            "Use the returned pair and builder metrics as execution-path context. "
            "The agent should compare pair sandwich pressure, recent builder exposure, "
            "trade size, and user slippage constraints before selecting a path candidate."
        )
        return RouteRecommendation(
            pair=pair,
            amount=amount,
            priority=priority,
            advisory=advisory,
            suggested_actions=suggested_route_actions(worst_pair_ratio, high_builder_rows),
            limitations=[
                "This is not a live routing guarantee.",
                "RPC provider attribution is not directly observable on-chain.",
                "Pool state and mempool conditions can change after historical Dune observations.",
            ],
            confidence=0.3,
            signal_source="historical_pair_and_builder_exposure_inferred_route_advisory",
            harm_proxy=HarmProxy(
                affected_notional_usd=0,
                method="route_advisory_risk_no_live_counterfactual",
                confidence=0.3,
            ),
            pair_context=pair_risk.rows[:20],
            builder_context=high_builder_rows[:20],
            evidence={
                "pair_token_risk": pair_risk.rows[:20],
                "builder_sandwich_exposure": high_builder_rows[:20],
            },
        )

    def execute_optional_wallet_query(
        self,
        key: str,
        parameters: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], str | None]:
        try:
            return self.runner.execute_registered_query(key, parameters), None
        except QueryUnavailable as exc:
            return [], str(exc)


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def summarize_top(
    rows: list[dict[str, Any]],
    key: str,
    value_key: str,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for row in rows:
        name = str(row.get(key) or "unknown")
        totals[name] = totals.get(name, 0.0) + float(row.get(value_key) or 0)
        counts[name] = counts.get(name, 0) + int(row.get("wallet_txs") or 0)
    return [
        {key: name, value_key: total, "wallet_txs": counts.get(name, 0)}
        for name, total in sorted(totals.items(), key=lambda item: item[1], reverse=True)[
            :limit
        ]
    ]


def suggested_route_actions(
    worst_pair_ratio: float,
    high_builder_rows: list[dict[str, Any]],
) -> list[str]:
    actions = [
        "Inspect pair-level sandwiched transaction and volume percentages.",
        "Compare recent builder exposure before selecting an execution path candidate.",
    ]
    if worst_pair_ratio > 0:
        actions.append("Consider tighter slippage, smaller order chunks, or delayed execution.")
    if high_builder_rows:
        actions.append(
            "Avoid over-relying on any single opaque path without fresh execution evidence."
        )
    actions.append(
        "Treat RPC path quality as inferred unless the transaction is submitted through RPCBeat."
    )
    return actions


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
