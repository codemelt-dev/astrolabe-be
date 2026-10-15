-- events: every contract and system event, fee and refund events included
-- (explorer.EventRow), ordered by contract for per-contract analytics.

-- +migration 5 events
CREATE TABLE IF NOT EXISTS events
(
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,                 -- 1-based
    op_index        Int32,                  -- 0-based; -1 for transaction-level (fee, refund) events
    event_index     UInt32,
    stage           LowCardinality(String), -- fee, refund or operation
    type            LowCardinality(String), -- contract or system
    contract_id     String,                 -- C…, or '' when absent
    topics          Array(String),          -- each JSON (ScValJSON); at most 4
    data            String,                 -- JSON (ScValJSON)
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (contract_id, ledger_sequence, tx_index, op_index, event_index);
