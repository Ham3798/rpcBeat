from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.models import (
    BuilderExposure,
    ExecutionAnalysis,
    Explanation,
    PairRisk,
    RouteRecommendation,
    WalletAnalysis,
)
from app.services.advisor import AdvisorService
from app.services.dune_queries import QueryUnavailable

router = APIRouter()


def advisor() -> AdvisorService:
    return AdvisorService()


ADVISOR_DEPENDENCY = Depends(advisor)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/wallet/{wallet}", response_model=WalletAnalysis)
def analyze_wallet(
    wallet: str,
    lookback_days: int = Query(default=30, ge=1, le=180),
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> WalletAnalysis:
    return call_service(lambda: service.analyze_wallet(wallet, lookback_days))


@router.get("/tx/{tx_hash}", response_model=ExecutionAnalysis)
def analyze_execution(
    tx_hash: str,
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> ExecutionAnalysis:
    return call_service(lambda: service.analyze_execution(tx_hash))


@router.get("/tx/{tx_hash}/explain", response_model=Explanation)
def explain_execution(
    tx_hash: str,
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> Explanation:
    return call_service(lambda: service.explain_execution(tx_hash))


@router.get("/builders/exposure", response_model=BuilderExposure)
def get_builder_mev_exposure(
    lookback_days: int = Query(default=7, ge=1, le=90),
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> BuilderExposure:
    return call_service(lambda: service.get_builder_mev_exposure(lookback_days))


@router.get("/pairs/{token_pair_or_addresses}/risk", response_model=PairRisk)
def get_pair_risk(
    token_pair_or_addresses: str,
    lookback_days: int = Query(default=30, ge=1, le=180),
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> PairRisk:
    return call_service(lambda: service.get_pair_risk(token_pair_or_addresses, lookback_days))


@router.get("/recommend-route", response_model=RouteRecommendation)
def recommend_route(
    pair: str,
    amount: float = Query(gt=0),
    priority: str = "safe",
    service: AdvisorService = ADVISOR_DEPENDENCY,
) -> RouteRecommendation:
    return call_service(lambda: service.recommend_route(pair, amount, priority))


def call_service(fn):
    try:
        return fn()
    except QueryUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
