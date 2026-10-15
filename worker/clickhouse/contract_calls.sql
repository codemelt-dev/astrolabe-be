-- contract_calls: each invoke_contract operation (explorer.ContractCallRow),
-- ordered by contract: a contract page's recent calls.

-- +migration 9 contract_calls
CREATE TABLE IF NOT EXISTS contract_calls
(
    contract_id     String,                 -- C…
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,
    op_index        UInt32,
    function        LowCardinality(String),
    caller          String,                 -- the operation's source
    successful      Bool,
    return_value    String,                 -- JSON (ScValJSON); '' when the call failed
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (contract_id, ledger_sequence, tx_index, op_index);
