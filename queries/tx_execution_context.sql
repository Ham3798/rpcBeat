WITH trades AS (
    SELECT
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.tx_from,
        t.tx_to,
        t.project,
        t.project_contract_address,
        t.token_pair,
        CASE
            WHEN t.tx_to IS NULL THEN 'unknown'
            WHEN t.project_contract_address IS NULL THEN 'unknown_contract'
            WHEN t.tx_to <> t.project_contract_address THEN 'dex_router_or_aggregator'
            WHEN t.tx_to = t.project_contract_address THEN 'direct_pool_or_project'
            ELSE 'unknown'
        END AS route_class,
        t.amount_usd,
        t.evt_index,
        CASE
            WHEN st.block_time IS NOT NULL THEN 'sandwiched'
            WHEN s.block_time IS NOT NULL THEN 'sandwich'
            ELSE 'other'
        END AS classification
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
        AND lower(CAST(t.tx_hash AS varchar)) = lower('{{tx_hash}}')
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
        AND txn.block_number IN (SELECT block_number FROM trades)
    GROUP BY 1, 2
),
block_context AS (
    SELECT
        b.number AS block_number,
        CAST(b.miner AS varchar) AS validator_address
    FROM bnb.blocks b
    WHERE b.number IN (SELECT block_number FROM trades)
)
SELECT
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.tx_from,
    t.tx_to,
    t.project,
    t.project_contract_address,
    t.token_pair,
    t.route_class,
    t.amount_usd,
    t.evt_index,
    t.classification,
    MAX(b.builder_brand) AS builder_brand,
    CASE
        WHEN MAX(b.builder_brand) IS NULL THEN 'none'
        ELSE 'zero_gas_tx_to_known_builder_address'
    END AS builder_attribution_basis,
    COALESCE(SUM(b.builder_marker_tx_count), 0) AS builder_marker_tx_count,
    MAX(bc.validator_address) AS validator_address,
    'block_miner_or_proposer' AS validator_role_label,
    'bnb.blocks.miner' AS validator_attribution_basis,
    CASE
        WHEN MAX(bc.validator_address) IS NULL THEN 'unknown'
        ELSE 'attributed'
    END AS validator_confidence
FROM trades t
LEFT JOIN builder_payment_marker_blocks b ON b.block_number = t.block_number
LEFT JOIN block_context bc ON bc.block_number = t.block_number
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
ORDER BY t.evt_index
