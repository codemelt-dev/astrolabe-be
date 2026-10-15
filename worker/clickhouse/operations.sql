-- operations: one row per operation (explorer.OperationRow). type_name is
-- the XDR's name in snake case (payment, invoke_host_function, …);
-- result_code the operation's own result.

-- +migration 4 operations
CREATE TABLE IF NOT EXISTS operations
(
    ledger_sequence      UInt32,
    closed_at            DateTime('UTC'),
    tx_index             UInt32,                 -- 1-based
    op_index             UInt32,                 -- 0-based
    id                   Int64,                  -- toid(ledger, tx_index, op_index + 1)
    type                 UInt8,                  -- xdr.OperationType
    type_name            LowCardinality(String),
    source_account       String,
    source_account_muxed String,
    details              String,                 -- JSON
    successful           Bool,                   -- the transaction's outcome
    result_code          LowCardinality(String), -- payment_success, payment_underfunded, op_bad_auth …; '' when the transaction failed before its operations ran
    return_value         String,                 -- a successful Soroban call's return value (JSON), else ''
    INDEX source_bf source_account TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (ledger_sequence, tx_index, op_index);
