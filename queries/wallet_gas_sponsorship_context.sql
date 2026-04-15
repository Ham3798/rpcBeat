WITH wallet_involved_trade_rows AS (
    SELECT
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.tx_from,
        t.tx_to,
        t.maker,
        t.taker,
        t.project_contract_address,
        CASE
            WHEN lower(CAST(t.tx_from AS varchar)) = lower('{{wallet}}') THEN 'direct_sender'
            WHEN lower(COALESCE(CAST(t.taker AS varchar), '')) = lower('{{wallet}}')
                OR lower(COALESCE(CAST(t.maker AS varchar), '')) = lower('{{wallet}}')
                THEN 'wallet_trade_non_sender'
            ELSE 'unknown'
        END AS sender_relation,
        CASE
            WHEN t.tx_to IS NULL THEN 'unknown'
            WHEN t.project_contract_address IS NULL THEN 'unknown_contract'
            WHEN t.tx_to <> t.project_contract_address THEN 'router_flow'
            WHEN t.tx_to = t.project_contract_address THEN 'direct_pool_or_project'
            ELSE 'unknown'
        END AS route_hint
    FROM dex.trades t
    WHERE t.blockchain = 'bnb'
        AND t.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND t.block_time < from_iso8601_timestamp('{{end_time}}')
        AND t.tx_to <> 0x6aba0315493b7e6989041c91181337b662fb1b90
        AND (
            lower(CAST(t.tx_from AS varchar)) = lower('{{wallet}}')
            OR lower(COALESCE(CAST(t.taker AS varchar), '')) = lower('{{wallet}}')
            OR lower(COALESCE(CAST(t.maker AS varchar), '')) = lower('{{wallet}}')
        )
),
direct_wallet_tx_gas AS (
    SELECT
        txn.hash AS tx_hash,
        txn.gas_price
    FROM bnb.transactions txn
    WHERE txn.block_time >= from_iso8601_timestamp('{{start_time}}')
        AND txn.block_time < from_iso8601_timestamp('{{end_time}}')
        AND lower(CAST(txn."from" AS varchar)) = lower('{{wallet}}')
),
wallet_involved_trade_rows_with_gas AS (
    SELECT
        rows.*,
        gas.gas_price
    FROM wallet_involved_trade_rows rows
    LEFT JOIN direct_wallet_tx_gas gas ON gas.tx_hash = rows.tx_hash
),
wallet_involved_txs AS (
    SELECT
        tx_hash,
        MAX(CASE WHEN sender_relation = 'direct_sender' THEN 1 ELSE 0 END) AS has_direct_sender,
        MAX(
            CASE WHEN sender_relation = 'direct_sender' AND gas_price = 0 THEN 1 ELSE 0 END
        ) AS has_gasless_direct_sender,
        MAX(CASE WHEN sender_relation = 'wallet_trade_non_sender' THEN 1 ELSE 0 END) AS has_wallet_trade_non_sender,
        MAX(CASE WHEN route_hint = 'router_flow' THEN 1 ELSE 0 END) AS has_router_flow
    FROM wallet_involved_trade_rows_with_gas
    GROUP BY 1
)
SELECT
    COUNT(DISTINCT tx_hash) AS wallet_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE has_direct_sender = 1) AS direct_sender_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE has_gasless_direct_sender = 1) AS gasless_direct_sender_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE has_wallet_trade_non_sender = 1) AS wallet_trade_non_sender_txs,
    COUNT(DISTINCT tx_hash) FILTER (WHERE has_router_flow = 1) AS router_flow_txs,
    COUNT(DISTINCT tx_hash) FILTER (
        WHERE has_gasless_direct_sender = 1
    ) AS possible_paymaster_gasless_candidate_txs,
    COUNT(DISTINCT tx_hash) FILTER (
        WHERE has_direct_sender = 0 AND has_wallet_trade_non_sender = 1
    ) AS possible_relayed_intent_candidate_txs,
    COUNT(DISTINCT tx_hash) FILTER (
        WHERE has_direct_sender = 0
            AND has_wallet_trade_non_sender = 0
    ) AS unknown_txs,
    false AS sponsorship_observed,
    CASE
        WHEN COUNT(DISTINCT tx_hash) = 0 THEN 'unknown'
        WHEN COUNT(DISTINCT tx_hash) FILTER (
            WHERE has_gasless_direct_sender = 1
                OR (has_direct_sender = 0 AND has_wallet_trade_non_sender = 1)
        ) > 0 THEN 'inferred'
        ELSE 'observed'
    END AS confidence
FROM wallet_involved_txs
