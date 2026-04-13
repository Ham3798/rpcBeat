WITH wallet_trades AS (
    SELECT
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.tx_from,
        t.tx_to,
        t.project,
        t.project_contract_address,
        t.token_pair,
        t.token_bought_address,
        t.token_sold_address,
        t.token_bought_symbol,
        t.token_sold_symbol,
        t.amount_usd,
        t.evt_index,
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
builder_blocks AS (
    SELECT
        txn.block_number,
        bld.brand AS builder_brand
    FROM bnb.transactions txn
    INNER JOIN dune.bnbchain.dataset_builders_list bld ON txn."to" = bld.address
    WHERE txn.success = true
        AND txn.gas_price = 0
        AND txn.block_number IN (SELECT block_number FROM wallet_trades)
),
pair_counts AS (
    SELECT
        token_pair,
        COUNT(DISTINCT tx_hash) AS txs
    FROM wallet_trades
    GROUP BY 1
),
top_pair_share AS (
    SELECT
        COALESCE(
            CAST(MAX(txs) AS double) / NULLIF(CAST(SUM(txs) AS double), 0),
            0
        ) AS high_risk_pair_share
    FROM pair_counts
),
builder_counts AS (
    SELECT
        bb.builder_brand,
        COUNT(DISTINCT wt.block_number) AS affected_blocks
    FROM wallet_trades wt
    INNER JOIN builder_blocks bb ON bb.block_number = wt.block_number
    WHERE wt.trade_type IN ('sandwiched', 'sandwich')
    GROUP BY 1
),
builder_concentration AS (
    SELECT
        COALESCE(
            CAST(MAX(affected_blocks) AS double) / NULLIF(CAST(SUM(affected_blocks) AS double), 0),
            0
        ) AS builder_concentration
    FROM builder_counts
)
SELECT
    COUNT(DISTINCT tx_hash) AS total_dex_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS sandwiched_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwich') AS sandwich_txs,
    COALESCE(SUM(amount_usd), 0) AS total_volume_usd,
    COALESCE(SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched'), 0) AS sandwiched_volume_usd,
    COALESCE((SELECT high_risk_pair_share FROM top_pair_share), 0) AS high_risk_pair_share,
    COALESCE((SELECT builder_concentration FROM builder_concentration), 0) AS builder_concentration
FROM wallet_trades
