WITH builder_payment_marker_blocks AS (
    SELECT
        txn.block_number,
        date_trunc('hour', txn.block_time) AS hour,
        bld.brand AS builder_brand,
        COUNT(*) AS builder_marker_tx_count
    FROM bnb.transactions txn
    INNER JOIN dune.bnbchain.dataset_builders_list bld ON txn."to" = bld.address
    WHERE txn.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND txn.block_time < from_iso8601_timestamp('{{end_time}}')
        AND txn.success = true
        AND txn.gas_price = 0
    GROUP BY 1, 2, 3
),
sandwich_txs AS (
    SELECT DISTINCT
        s.block_number,
        s.tx_hash,
        'sandwich' AS mev_type
    FROM dex.sandwiches s
    WHERE s.blockchain = 'bnb'
        AND s.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND s.block_time < from_iso8601_timestamp('{{end_time}}')
        AND s.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
    UNION ALL
    SELECT DISTINCT
        st.block_number,
        st.tx_hash,
        'sandwiched' AS mev_type
    FROM dex.sandwiched st
    WHERE st.blockchain = 'bnb'
        AND st.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND st.block_time < from_iso8601_timestamp('{{end_time}}')
        AND st.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
),
builder_hourly AS (
    SELECT
        hour,
        builder_brand,
        COUNT(DISTINCT block_number) AS builder_attributed_blocks,
        SUM(builder_marker_tx_count) AS builder_marker_tx_count
    FROM builder_payment_marker_blocks
    GROUP BY 1, 2
),
mev_hourly AS (
    SELECT
        b.hour,
        b.builder_brand,
        COUNT(DISTINCT m.tx_hash) FILTER (WHERE m.mev_type = 'sandwich') AS sandwich_tx_count,
        COUNT(DISTINCT m.tx_hash) FILTER (WHERE m.mev_type = 'sandwiched') AS sandwiched_tx_count,
        COUNT(DISTINCT b.block_number) FILTER (WHERE m.tx_hash IS NOT NULL) AS affected_blocks
    FROM builder_payment_marker_blocks b
    LEFT JOIN sandwich_txs m ON m.block_number = b.block_number
    GROUP BY 1, 2
)
SELECT
    b.hour,
    b.builder_brand,
    'zero_gas_tx_to_known_builder_address' AS builder_attribution_basis,
    b.builder_marker_tx_count,
    CAST(NULL AS varchar) AS validator_address,
    'unknown' AS validator_confidence,
    COALESCE(m.sandwich_tx_count, 0) AS sandwich_tx_count,
    COALESCE(m.sandwiched_tx_count, 0) AS sandwiched_tx_count,
    COALESCE(m.affected_blocks, 0) AS affected_blocks
FROM builder_hourly b
LEFT JOIN mev_hourly m ON m.hour = b.hour
    AND m.builder_brand = b.builder_brand
ORDER BY 1, 2
