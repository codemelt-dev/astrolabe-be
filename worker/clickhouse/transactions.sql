-- transactions: one row per transaction (explorer.TransactionRow), with its
-- ledger's close time.

-- +migration 3 transactions
CREATE TABLE IF NOT EXISTS transactions
(
    ledger_sequence      UInt32,
    closed_at            DateTime('UTC'),
    tx_index             UInt32,                 -- 1-based
    id                   Int64,                  -- toid(ledger, tx_index, 0)
    hash                 FixedString(32),
    inner_hash           FixedString(32),        -- zeros unless a fee bump
    source_account       String,
    source_account_muxed String,
    account_sequence     Int64,
    fee_account          String,
    fee_account_muxed    String,
    max_fee              Int64,
    fee_charged          Int64,
    successful           Bool,
    result_code          Int32,
    op_count             UInt32,
    memo_type            LowCardinality(String),
    memo                 String,
    soroban              Bool,
    INDEX hash_bf hash TYPE bloom_filter GRANULARITY 4,
    INDEX source_bf source_account TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (ledger_sequence, tx_index);
