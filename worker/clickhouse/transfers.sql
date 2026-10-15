-- token_transfers: the SDK's token transfer events (explorer.TransferRow),
-- ordered by token. transfers_by_account
-- holds each transfer twice, once per side, ordered by account; the
-- materialized view fills it from every insert into token_transfers, and
-- the sink's delete above the cursor covers it too. The view names
-- token_transfers.amount: its own amount alias is visible in the whole
-- query, ARRAY JOIN included.

-- +migration 6 token_transfers
CREATE TABLE IF NOT EXISTS token_transfers
(
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,                 -- 1-based
    op_index        Int32,                  -- 0-based; -1 for fee transfers
    position        UInt32,                 -- order within (tx_index, op_index)
    kind            LowCardinality(String), -- transfer, mint, burn, clawback or fee
    asset           String,                 -- native, CODE:ISSUER, or '' for non-SAC tokens
    contract_id     String,                 -- the token contract (C…)
    from_address    String,                 -- '' for mints
    to_address      String,                 -- '' for burns
    amount          Int128,                 -- base units; negative for a fee refund
    to_muxed_info   String,                 -- JSON, or ''
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1,
    INDEX from_bf from_address TYPE bloom_filter GRANULARITY 4,
    INDEX to_bf to_address TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (contract_id, ledger_sequence, tx_index, op_index, position);

CREATE TABLE IF NOT EXISTS transfers_by_account
(
    account         String,
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,
    op_index        Int32,
    position        UInt32,
    kind            LowCardinality(String),
    contract_id     String,
    asset           String,
    counterparty    String,
    amount          Int128,                 -- negative when the account sends
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (account, ledger_sequence, tx_index, op_index, position);

CREATE MATERIALIZED VIEW IF NOT EXISTS transfers_by_account_mv TO transfers_by_account AS
SELECT side.1 AS account, ledger_sequence, closed_at, tx_index, op_index, position,
       kind, contract_id, asset, side.2 AS counterparty, side.3 AS amount
FROM token_transfers
ARRAY JOIN [(from_address, to_address, -token_transfers.amount), (to_address, from_address, token_transfers.amount)] AS side
WHERE side.1 != '';
