-- ingest_state: each pipeline's cursor, the last committed ledger and its
-- hash. The sink inserts one row per commit, after the batch's rows;
-- readers take the row with the highest ledger (ReadCursor). Rows of
-- history tables above the cursor are never trusted (Sink.cleanAbove).

-- +migration 1 ingest_state
CREATE TABLE IF NOT EXISTS ingest_state
(
    pipeline        LowCardinality(String),
    ledger_sequence UInt32,
    ledger_hash     FixedString(32),
    updated_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ledger_sequence)
ORDER BY pipeline;
