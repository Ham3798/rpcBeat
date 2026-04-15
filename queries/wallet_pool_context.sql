WITH wallet_trades AS (
    SELECT
        t.block_time,
        t.tx_hash,
        t.project,
        t.project_contract_address,
        t.token_pair,
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
wallet_pools AS (
    SELECT DISTINCT project_contract_address
    FROM wallet_trades
),
pool_trades AS (
    SELECT
        t.tx_hash,
        t.project_contract_address,
        t.amount_usd,
        CASE
            WHEN st.block_time IS NOT NULL THEN 'sandwiched'
            WHEN s.block_time IS NOT NULL THEN 'sandwich'
            ELSE 'other'
        END AS trade_type
    FROM dex.trades t
    INNER JOIN wallet_pools wp ON wp.project_contract_address = t.project_contract_address
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
        AND t.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
),
wallet_aggregated AS (
    SELECT
        project,
        project_contract_address,
        token_pair,
        COUNT(DISTINCT tx_hash) AS wallet_txs,
        COALESCE(SUM(amount_usd), 0) AS wallet_volume_usd,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS wallet_sandwiched_txs,
        COALESCE(SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched'), 0) AS wallet_sandwiched_volume_usd
    FROM wallet_trades
    GROUP BY 1, 2, 3
),
pool_aggregated AS (
    SELECT
        project_contract_address,
        COALESCE(SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched'), 0) AS pool_sandwiched_volume,
        COALESCE(SUM(amount_usd) FILTER (WHERE trade_type <> 'sandwich'), 0) AS pool_non_attacker_volume,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS pool_sandwiched_transactions,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type <> 'sandwich') AS pool_non_attacker_transactions
    FROM pool_trades
    GROUP BY 1
)
SELECT
    w.project,
    w.project_contract_address,
    w.token_pair,
    w.wallet_txs,
    w.wallet_volume_usd,
    w.wallet_sandwiched_txs,
    w.wallet_sandwiched_volume_usd,
    COALESCE(
        CAST(p.pool_sandwiched_transactions AS double)
            / NULLIF(CAST(p.pool_non_attacker_transactions AS double), 0),
        0
    ) AS pool_sandwiched_transactions_percentage,
    COALESCE(
        p.pool_sandwiched_volume / NULLIF(p.pool_non_attacker_volume, 0),
        0
    ) AS pool_sandwiched_volume_percentage
FROM wallet_aggregated w
LEFT JOIN pool_aggregated p ON p.project_contract_address = w.project_contract_address
ORDER BY w.wallet_sandwiched_volume_usd DESC, w.wallet_volume_usd DESC
LIMIT 50
