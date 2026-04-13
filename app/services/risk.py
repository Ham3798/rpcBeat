from __future__ import annotations

from app.models import RiskLevel, RiskScore


def risk_level(score: int) -> RiskLevel:
    if score >= 80:
        return RiskLevel.critical
    if score >= 60:
        return RiskLevel.high
    if score >= 30:
        return RiskLevel.medium
    return RiskLevel.low


def wallet_risk_score(
    *,
    total_dex_txs: int,
    sandwiched_txs: int,
    sandwiched_volume_usd: float,
    total_volume_usd: float,
    high_risk_pair_share: float,
    builder_concentration: float,
) -> RiskScore:
    sandwiched_tx_ratio = safe_ratio(sandwiched_txs, total_dex_txs)
    sandwiched_volume_ratio = safe_ratio(sandwiched_volume_usd, total_volume_usd)
    components = {
        "sandwiched_tx_ratio": clamp01(sandwiched_tx_ratio) * 40,
        "sandwiched_volume_ratio": clamp01(sandwiched_volume_ratio) * 25,
        "high_risk_pair_exposure": clamp01(high_risk_pair_share) * 20,
        "builder_exposure_concentration": clamp01(builder_concentration) * 15,
    }
    score = round(sum(components.values()))
    return RiskScore(score=score, level=risk_level(score), components=components)


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))

