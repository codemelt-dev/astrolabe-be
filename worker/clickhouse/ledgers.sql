-- ledgers: one row per ledger (explorer.LedgerRow). Hashes are raw bytes:
-- lower(hex(hash)) prints them.

-- +migration 2 ledgers
CREATE TABLE IF NOT EXISTS ledgers
(
    sequence            UInt32,
    hash                FixedString(32),
    prev_hash           FixedString(32),
    closed_at           DateTime('UTC'),
    protocol_version    UInt32,
    tx_count            UInt32,
    failed_tx_count     UInt32,
    op_count            UInt32,
    successful_op_count UInt32,
    base_fee            UInt32,
    base_reserve        UInt32,
    max_tx_set_size     UInt32,
    total_coins         Int64,
    fee_pool            Int64,
    soroban_state_size  UInt64,                 -- 0 when the ledger does not report it
    source              LowCardinality(String), -- galexie or aws_lake: where the ledger was read
    core_version        LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY sequence;
