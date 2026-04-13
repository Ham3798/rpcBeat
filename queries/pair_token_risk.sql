WITH trades AS (
    SELECT
        t.blockchain,
        t.project,
        t.version,
        t.token_pair,
        t.block_time,
        t.token_bought_address,
        t.token_sold_address,
        t.token_bought_symbol,
        t.token_sold_symbol,
        t.tx_from,
        t.amount_usd,
        t.tx_hash,
        t.project_contract_address,
        CASE
            WHEN s.block_time IS NOT NULL THEN 'sandwich'
            WHEN st.block_time IS NOT NULL THEN 'sandwiched'
            ELSE 'other'
        END AS trade_type
    FROM dex.trades t
    LEFT JOIN dex.sandwiches s ON s.blockchain = 'bnb'
        AND s.block_time = t.block_time
        AND s.tx_hash = t.tx_hash
        AND s.project_contract_address = t.project_contract_address
        AND s.evt_index = t.evt_index
    LEFT JOIN dex.sandwiched st ON st.blockchain = 'bnb'
        AND st.block_time = t.block_time
        AND st.tx_hash = t.tx_hash
        AND st.project_contract_address = t.project_contract_address
        AND st.evt_index = t.evt_index
    WHERE t.blockchain = 'bnb'
        AND t.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND t.block_time < from_iso8601_timestamp('{{end_time}}')
        AND (
            lower(COALESCE(t.token_pair, '')) LIKE '%' || lower('{{token_pair_or_addresses}}') || '%'
            OR lower(CAST(t.token_bought_address AS varchar)) = lower('{{token_pair_or_addresses}}')
            OR lower(CAST(t.token_sold_address AS varchar)) = lower('{{token_pair_or_addresses}}')
        )
),
aggregated AS (
    SELECT
        token_pair,
        SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched') AS sandwiched_volume,
        SUM(amount_usd) FILTER (WHERE trade_type <> 'sandwich') AS non_attacker_volume,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS sandwiched_transactions,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type <> 'sandwich') AS non_attacker_transactions,
        COUNT(DISTINCT tx_from) FILTER (WHERE trade_type = 'sandwiched') AS sandwiched_traders,
        COUNT(DISTINCT tx_from) FILTER (WHERE trade_type <> 'sandwich') AS non_attacker_traders
    FROM trades
    GROUP BY 1
)
SELECT
    token_pair,
    COALESCE(sandwiched_volume, 0) AS sandwiched_volume,
    COALESCE(sandwiched_volume / NULLIF(non_attacker_volume, 0), 0) AS sandwiched_volume_percentage,
    sandwiched_transactions,
    COALESCE(
        CAST(sandwiched_transactions AS double) / NULLIF(CAST(non_attacker_transactions AS double), 0),
        0
    ) AS sandwiched_transactions_percentage,
    sandwiched_traders,
    COALESCE(
        CAST(sandwiched_traders AS double) / NULLIF(CAST(non_attacker_traders AS double), 0),
        0
    ) AS sandwiched_traders_percentage
FROM aggregated
WHERE sandwiched_transactions > 0
ORDER BY sandwiched_volume DESC
LIMIT 50

