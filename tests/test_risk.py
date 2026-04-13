from app.models import RiskLevel
from app.services.risk import wallet_risk_score


def test_wallet_risk_score_boundaries() -> None:
    low = wallet_risk_score(
        total_dex_txs=10,
        sandwiched_txs=0,
        sandwiched_volume_usd=0,
        total_volume_usd=1000,
        high_risk_pair_share=0,
        builder_concentration=0,
    )
    critical = wallet_risk_score(
        total_dex_txs=10,
        sandwiched_txs=10,
        sandwiched_volume_usd=1000,
        total_volume_usd=1000,
        high_risk_pair_share=1,
        builder_concentration=1,
    )

    assert low.score == 0
    assert low.level == RiskLevel.low
    assert critical.score == 100
    assert critical.level == RiskLevel.critical

