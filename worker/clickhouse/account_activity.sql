-- account_activity: every transaction an account or contract took part in
-- (explorer.ActivityRow), ordered by address: an account page's history,
-- newest first, reads one range. roles is a bitmask: 1 the transaction's
-- source, 2 its fee payer, 4 an operation's source, 8 named in an
-- operation's body, 16 a side of a token transfer, 32 owns a ledger entry
-- the transaction changed.

-- +migration 8 account_activity
CREATE TABLE IF NOT EXISTS account_activity
(
    account         String,                 -- G… or C…
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,
    roles           UInt8,
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (account, ledger_sequence, tx_index);
