WITH builder_blocks AS (
    SELECT
        txn.block_number,
        txn.block_time,
        bld.brand AS builder_brand
    FROM bnb.transactions txn
    INNER JOIN dune.bnbchain.dataset_builders_list bld ON txn."to" = bld.address
    WHERE txn.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND txn.block_time < from_iso8601_timestamp('{{end_time}}')
        AND txn.success = true
        AND txn.gas_price = 0
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
)
SELECT
    date_trunc('hour', b.block_time) AS hour,
    b.builder_brand,
    CAST(NULL AS varchar) AS validator_address,
    'unknown' AS validator_confidence,
    COUNT(DISTINCT m.tx_hash) FILTER (WHERE m.mev_type = 'sandwich') AS sandwich_tx_count,
    COUNT(DISTINCT m.tx_hash) FILTER (WHERE m.mev_type = 'sandwiched') AS sandwiched_tx_count,
    COUNT(DISTINCT b.block_number) FILTER (WHERE m.tx_hash IS NOT NULL) AS affected_blocks
FROM builder_blocks b
LEFT JOIN sandwich_txs m ON m.block_number = b.block_number
GROUP BY 1, 2
ORDER BY 1, 2
