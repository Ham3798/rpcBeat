from __future__ import annotations

import json
import re
from pathlib import Path


def test_each_sql_has_metadata_and_required_params() -> None:
    query_dir = Path("queries")
    for sql_path in query_dir.glob("*.sql"):
        metadata_path = sql_path.with_suffix(".json")
        assert metadata_path.exists(), f"missing metadata for {sql_path}"
        metadata = json.loads(metadata_path.read_text())
        assert metadata["required_columns"], f"missing required_columns for {sql_path}"
        params = set(re.findall(r"\{\{([a-zA-Z0-9_]+)\}\}", sql_path.read_text()))
        if sql_path.stem in {"wallet_mev_exposure"}:
            assert {"wallet", "start_time", "end_time"} <= params
        if sql_path.stem in {"tx_execution_context"}:
            assert {"tx_hash", "start_time", "end_time"} <= params
        if sql_path.stem in {
            "builder_sandwich_exposure",
            "block_trade_mev_share",
            "emerging_sandwich_type2",
        }:
            assert {"start_time", "end_time"} <= params
        if sql_path.stem == "pair_token_risk":
            assert {"start_time", "end_time", "token_pair_or_addresses"} <= params
