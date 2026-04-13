WITH details AS (
    SELECT
        *,
        CASE
            WHEN token_bought_symbol IN ('USDT', 'WBNB') THEN token_sold_amount
            WHEN token_sold_symbol IN ('USDT', 'WBNB') THEN token_bought_amount
        END AS normalized_amount
    FROM dex.trades
    WHERE blockchain = 'bnb'
        AND block_time >= from_iso8601_timestamp('{{start_time}}')
        AND block_time < from_iso8601_timestamp('{{end_time}}')
        AND tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
),
indexed_sandwich_trades AS (
    SELECT DISTINCT
        front.block_time,
        t.tx_hash_all AS tx_hash,
        front.project,
        front.version,
        front.project_contract_address,
        t.evt_index_all AS evt_index
    FROM details front
    INNER JOIN details back ON back.blockchain = 'bnb'
        AND front.block_time = back.block_time
        AND front.project_contract_address = back.project_contract_address
        AND front.tx_from <> back.tx_from
        AND front.tx_hash <> back.tx_hash
        AND front.token_sold_address = back.token_bought_address
        AND front.token_bought_address = back.token_sold_address
        AND front.evt_index + 1 < back.evt_index
        AND front.normalized_amount = back.normalized_amount
    INNER JOIN details victim ON victim.blockchain = 'bnb'
        AND front.block_time = victim.block_time
        AND front.project_contract_address = victim.project_contract_address
        AND front.tx_from <> victim.tx_from
        AND front.tx_to <> victim.tx_to
        AND back.tx_to <> victim.tx_to
        AND front.token_bought_address = victim.token_bought_address
        AND front.token_sold_address = victim.token_sold_address
        AND victim.evt_index BETWEEN front.evt_index AND back.evt_index
    CROSS JOIN UNNEST(
        ARRAY[
            (front.tx_hash, front.evt_index),
            (back.tx_hash, back.evt_index)
        ]
    ) AS t(tx_hash_all, evt_index_all)
)
SELECT
    dt.blockchain,
    dt.project,
    dt.version,
    dt.block_time,
    dt.block_number,
    dt.token_sold_address,
    dt.token_bought_address,
    dt.token_sold_symbol,
    dt.token_bought_symbol,
    dt.tx_hash,
    dt.tx_from,
    dt.tx_to,
    dt.project_contract_address,
    dt.token_pair,
    dt.token_sold_amount,
    dt.token_bought_amount,
    dt.amount_usd,
    dt.evt_index
FROM details dt
INNER JOIN indexed_sandwich_trades s ON dt.block_time = s.block_time
    AND dt.tx_hash = s.tx_hash
    AND dt.project_contract_address = s.project_contract_address
    AND dt.evt_index = s.evt_index

