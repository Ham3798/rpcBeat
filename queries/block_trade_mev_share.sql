WITH trades_and_mev AS (
    SELECT
        dt.block_time,
        CASE
            WHEN MAX(ds.project_contract_address) IS NOT NULL THEN 'MEV'
            ELSE 'Trades (no MEV)'
        END AS category
    FROM dex.trades dt
    LEFT JOIN dex.sandwiches ds ON ds.blockchain = 'bnb'
        AND ds.block_time = dt.block_time
        AND ds.project_contract_address = dt.project_contract_address
        AND ds.evt_index = dt.evt_index
    WHERE dt.blockchain = 'bnb'
        AND dt.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND dt.block_time < from_iso8601_timestamp('{{end_time}}')
        AND dt.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
    GROUP BY 1
),
block_data AS (
    SELECT
        b.time AS block_time,
        b.number AS block_number,
        COALESCE(tam.category, 'No Trades') AS category
    FROM bnb.blocks b
    LEFT JOIN trades_and_mev tam ON b.time = tam.block_time
    WHERE b.time >= from_iso8601_timestamp('{{start_time}}')
        AND b.time < from_iso8601_timestamp('{{end_time}}')
)
SELECT
    date_trunc('hour', block_time) AS hour,
    category,
    COUNT(DISTINCT block_number) AS block_count
FROM block_data
GROUP BY 1, 2
ORDER BY 1, 2

