from __future__ import annotations

import json
import re
from pathlib import Path


def test_each_sql_has_metadata_and_required_params() -> None:
    query_dir = Path("queries")
    for sql_path in query_dir.rglob("*.sql"):
        if "archive" in sql_path.parts:
            continue
        metadata_path = sql_path.with_suffix(".json")
        assert metadata_path.exists(), f"missing metadata for {sql_path}"
        metadata = json.loads(metadata_path.read_text())
        assert metadata["required_columns"], f"missing required_columns for {sql_path}"
        params = set(re.findall(r"\{\{([a-zA-Z0-9_]+)\}\}", sql_path.read_text()))
        if sql_path.stem in {
            "wallet_mev_exposure",
            "wallet_builder_context",
            "wallet_pool_context",
            "wallet_route_context",
            "wallet_gas_sponsorship_context",
        }:
            assert {"wallet", "start_time", "end_time"} <= params
        if sql_path.stem == "tx_execution_context":
            assert {"tx_hash", "start_time", "end_time"} <= params
        if sql_path.stem == "builder_sandwich_exposure":
            assert {"start_time", "end_time"} <= params
        if sql_path.stem == "pair_token_risk":
            assert {"start_time", "end_time", "token_pair_or_addresses"} <= params
        if sql_path.stem in {
            "wallet_route_context",
            "wallet_gas_sponsorship_context",
        }:
            assert "confidence" in metadata["required_columns"]


def test_builder_context_queries_expose_block_level_marker_basis() -> None:
    for query_key in {
        "builder_sandwich_exposure",
        "wallet_builder_context",
        "tx_execution_context",
        "wallet_mev_exposure",
    }:
        metadata = json.loads(Path(f"queries/{query_key}.json").read_text())
        required_columns = set(metadata["required_columns"])
        assert "builder_attribution_basis" in required_columns
        assert "builder_marker_tx_count" in required_columns


def test_validator_context_queries_expose_block_level_miner_basis() -> None:
    for query_path in {
        Path("queries/tx_execution_context.json"),
    }:
        metadata = json.loads(query_path.read_text())
        required_columns = set(metadata["required_columns"])
        assert "validator_address" in required_columns
        assert "validator_role_label" in required_columns
        assert "validator_attribution_basis" in required_columns
        assert "validator_confidence" in required_columns


def test_wallet_sender_gasless_context_avoids_confirmed_sponsorship_claim() -> None:
    metadata = json.loads(Path("queries/wallet_gas_sponsorship_context.json").read_text())
    required_columns = set(metadata["required_columns"])
    assert {
        "gasless_direct_sender_txs",
        "wallet_trade_non_sender_txs",
        "possible_paymaster_gasless_candidate_txs",
        "possible_relayed_intent_candidate_txs",
        "sponsorship_observed",
    } <= required_columns
    assert "possible_relayed_or_sponsored_txs" not in required_columns
