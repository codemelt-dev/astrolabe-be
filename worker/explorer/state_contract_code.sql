-- contract_code: uploaded Wasm, current state. wasm is the code itself (all
-- of pubnet's is ~52 MB); spec is its contractspecv0 section (the functions,
-- types, events and errors it declares) as JSON, NULL when the code has none.
-- Archived code stays, so a contract's functions are known after eviction.

-- +migration 6 state_contract_code
CREATE TABLE explorer.contract_code (
    key_hash             BYTEA   PRIMARY KEY, -- sha256(XDR(LedgerKey))
    code_hash            BYTEA   NOT NULL,
    wasm_size            INTEGER NOT NULL,
    wasm                 BYTEA   NOT NULL,
    spec                 JSONB,
    archived             BOOLEAN NOT NULL DEFAULT false,
    last_modified_ledger BIGINT  NOT NULL
);
