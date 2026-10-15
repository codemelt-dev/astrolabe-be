-- contract_creations: who created each contract, and when
-- (explorer.ContractCreationRow): every instance entry a transaction
-- created, so contracts deployed by other contracts are in it too.

-- +migration 10 contract_creations
CREATE TABLE IF NOT EXISTS contract_creations
(
    contract_id     String,                 -- C…
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,
    op_index        UInt32,
    creator         String,                 -- the operation's source
    via_contract    String,                 -- the contract that deployed it; '' for a create_contract operation
    executable      LowCardinality(String), -- wasm or stellar_asset
    wasm_hash       FixedString(32),        -- zeros for a Stellar Asset Contract
    asset           String,                 -- a Stellar Asset Contract's asset
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY contract_id;
