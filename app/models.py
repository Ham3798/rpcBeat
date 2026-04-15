from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RiskLevel(StrEnum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class LossEstimate(BaseModel):
    amount_usd: float = 0.0
    confidence: float = Field(default=0.35, ge=0.0, le=1.0)
    method: str = "conservative_sandwiched_amount_usd"


class HarmProxy(BaseModel):
    affected_notional_usd: float = 0.0
    method: str = "affected_sandwiched_notional_usd"
    confidence: float = Field(default=0.35, ge=0.0, le=1.0)
    limitations: str = (
        "Affected notional is not a counterfactual realized loss. "
        "RPC provider identity is not directly observable on-chain."
    )


class WalletAnalysis(BaseModel):
    wallet: str
    lookback_days: int
    wallet_orderflow: dict[str, Any]
    wallet_mev_harm: dict[str, Any]
    builder_context: dict[str, Any]
    pool_context: dict[str, Any]
    validator_context: dict[str, Any]
    rpc_path_inference: dict[str, Any]
    confidence: float = Field(default=0.35, ge=0.0, le=1.0)
    signal_source: str = "dune_curated_sandwich_tables"
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class ExecutionAnalysis(BaseModel):
    tx_hash: str
    classification: str
    block_number: int | None = None
    block_time: datetime | None = None
    route_class: str | None = None
    builder_brand: str | None = None
    validator_address: str | None = None
    validator_role_label: str | None = None
    validator_attribution_basis: str | None = None
    validator_confidence: str = "unknown"
    amount_usd: float = 0.0
    estimated_loss: LossEstimate
    harm_proxy: HarmProxy
    confidence: float = Field(default=0.35, ge=0.0, le=1.0)
    signal_source: str = "dune_curated_sandwich_tables"
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class Explanation(BaseModel):
    tx_hash: str
    summary: str
    risk_level: RiskLevel
    evidence: list[dict[str, Any]]


class BuilderExposure(BaseModel):
    lookback_days: int
    rows: list[dict[str, Any]]


class PairRisk(BaseModel):
    token_pair_or_addresses: str
    lookback_days: int
    rows: list[dict[str, Any]]


class RouteRecommendation(BaseModel):
    pair: str
    amount: float
    priority: str
    advisory: str
    suggested_actions: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.25, ge=0.0, le=1.0)
    signal_source: str = "historical_pair_risk_advisory"
    harm_proxy: HarmProxy = Field(default_factory=HarmProxy)
    pair_context: list[dict[str, Any]] = Field(default_factory=list)
    builder_context: list[dict[str, Any]] = Field(default_factory=list)
    evidence: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
