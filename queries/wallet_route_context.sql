WITH wallet_trades AS (
    SELECT
        t.block_time,
        t.tx_hash,
        t.tx_from,
        t.tx_to,
        t.project,
        t.project_contract_address,
        t.amount_usd,
        CASE
            WHEN t.tx_to IS NULL THEN 'unknown'
            WHEN t.project_contract_address IS NULL THEN 'unknown_contract'
            WHEN t.tx_to <> t.project_contract_address THEN 'dex_router_or_aggregator'
            WHEN t.tx_to = t.project_contract_address THEN 'direct_pool_or_project'
            ELSE 'unknown'
        END AS route_class,
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
route_totals AS (
    SELECT
        route_class,
        COUNT(DISTINCT tx_hash) AS wallet_txs,
        COALESCE(SUM(amount_usd), 0) AS wallet_volume_usd,
        COUNT(DISTINCT tx_hash) FILTER (WHERE trade_type = 'sandwiched') AS sandwiched_txs,
        COALESCE(SUM(amount_usd) FILTER (WHERE trade_type = 'sandwiched'), 0) AS sandwiched_volume_usd
    FROM wallet_trades
    GROUP BY 1
),
tx_to_counts AS (
    SELECT
        route_class,
        COALESCE(CAST(tx_to AS varchar), 'unknown') AS tx_to_text,
        COUNT(DISTINCT tx_hash) AS txs
    FROM wallet_trades
    GROUP BY 1, 2
),
top_tx_to AS (
    SELECT
        route_class,
        max_by(tx_to_text, txs) AS top_tx_to
    FROM tx_to_counts
    GROUP BY 1
),
project_counts AS (
    SELECT
        route_class,
        COALESCE(project, 'unknown') AS project,
        COUNT(DISTINCT tx_hash) AS txs
    FROM wallet_trades
    GROUP BY 1, 2
),
top_projects AS (
    SELECT
        route_class,
        array_join(slice(array_agg(project ORDER BY txs DESC), 1, 5), ', ') AS top_projects
    FROM project_counts
    GROUP BY 1
)
SELECT
    rt.route_class,
    rt.wallet_txs,
    rt.wallet_volume_usd,
    rt.sandwiched_txs,
    rt.sandwiched_volume_usd,
    COALESCE(tt.top_tx_to, 'unknown') AS top_tx_to,
    COALESCE(tp.top_projects, 'unknown') AS top_projects,
    CASE
        WHEN rt.route_class = 'unknown' THEN 'unknown'
        ELSE 'inferred'
    END AS confidence
FROM route_totals rt
LEFT JOIN top_tx_to tt ON tt.route_class = rt.route_class
LEFT JOIN top_projects tp ON tp.route_class = rt.route_class
ORDER BY rt.sandwiched_volume_usd DESC, rt.wallet_volume_usd DESC
