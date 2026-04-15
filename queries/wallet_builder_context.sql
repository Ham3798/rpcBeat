WITH wallet_trades AS (
    SELECT
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.amount_usd,
        CASE
            WHEN st.block_time IS NOT NULL THEN 'sandwiched'
            WHEN s.block_time IS NOT NULL THEN 'sandwich'
            ELSE 'other'
        END AS trade_type
    FROM dex.trades t
    LEFT JOIN dex.sandwiched st ON st.blockchain = 'bnb'
        AND st.block_time = t.block_time
        AND st.tx_hash = t.tx_hash
        AND st.project_contract_address = t.project_contract_address
        AND st.evt_index = t.evt_index
    LEFT JOIN dex.sandwiches s ON s.blockchain = 'bnb'
        AND s.block_time = t.block_time
        AND s.tx_hash = t.tx_hash
        AND s.project_contract_address = t.project_contract_address
        AND s.evt_index = t.evt_index
    WHERE t.blockchain = 'bnb'
        AND t.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND t.block_time < from_iso8601_timestamp('{{end_time}}')
        AND lower(CAST(t.tx_from AS varchar)) = lower('{{wallet}}')
        AND t.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
),
builder_payment_marker_blocks AS (
    SELECT
        txn.block_number,
        bld.brand AS builder_brand,
        COUNT(*) AS builder_marker_tx_count
    FROM bnb.transactions txn
    INNER JOIN dune.bnbchain.dataset_builders_list bld ON txn."to" = bld.address
    WHERE txn.success = true
        AND txn.gas_price = 0
        AND txn.block_number IN (SELECT block_number FROM wallet_trades)
    GROUP BY 1, 2
),
joined AS (
    SELECT
        COALESCE(bb.builder_brand, 'unknown') AS builder_brand,
        wt.block_number,
        wt.tx_hash,
        wt.amount_usd,
        wt.trade_type
    FROM wallet_trades wt
    LEFT JOIN builder_payment_marker_blocks bb ON bb.block_number = wt.block_number
),
totals AS (
    SELECT
        COUNT(DISTINCT block_number) AS total_wallet_blocks
    FROM joined
),
aggregated AS (
    SELECT
        builder_brand,
        COUNT(DISTINCT block_number) AS wallet_blocks,
        COUNT(DISTINCT tx_hash) AS wallet_txs,
        COALESCE(SUM(amount_usd), 0) AS wallet_volume_usd,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS wallet_sandwiched_txs,
        COALESCE(SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched'), 0) AS wallet_sandwiched_volume_usd,
        COUNT(DISTINCT block_number) FILTER (WHERE trade_type IN ('sandwiched', 'sandwich')) AS affected_blocks
    FROM joined
    GROUP BY 1
),
marker_counts AS (
    SELECT
        builder_brand,
        SUM(builder_marker_tx_count) AS builder_marker_tx_count
    FROM builder_payment_marker_blocks
    GROUP BY 1
)
SELECT
    a.builder_brand,
    CASE
        WHEN a.builder_brand = 'unknown' THEN 'none'
        ELSE 'zero_gas_tx_to_known_builder_address'
    END AS builder_attribution_basis,
    COALESCE(mc.builder_marker_tx_count, 0) AS builder_marker_tx_count,
    a.wallet_blocks,
    a.wallet_txs,
    a.wallet_volume_usd,
    a.wallet_sandwiched_txs,
    a.wallet_sandwiched_volume_usd,
    COALESCE(CAST(a.wallet_blocks AS double) / NULLIF(CAST(t.total_wallet_blocks AS double), 0), 0) AS wallet_block_share,
    COALESCE(CAST(a.affected_blocks AS double) / NULLIF(CAST(a.wallet_blocks AS double), 0), 0) AS affected_block_share,
    CASE WHEN a.builder_brand = 'unknown' THEN 'unknown' ELSE 'attributed' END AS attribution_confidence
FROM aggregated a
CROSS JOIN totals t
LEFT JOIN marker_counts mc ON mc.builder_brand = a.builder_brand
ORDER BY a.wallet_sandwiched_volume_usd DESC, a.wallet_volume_usd DESC
LIMIT 50
