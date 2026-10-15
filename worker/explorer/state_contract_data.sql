-- contract_data: current state.
-- key_hash = sha256(XDR(LedgerKey)) (= TtlEntry.KeyHash). archived = evicted
-- after the bootstrap (TEMPORARY entries are deleted on eviction instead).
-- A contract's instance entry (key "ledger_key_contract_instance") also gets
-- executable, wasm_hash and, for a Stellar Asset Contract, asset: one row per
-- contract, which api.contracts lists.

-- +migration 5 state_contract_data
CREATE TABLE explorer.contract_data (
    key_hash             BYTEA   PRIMARY KEY,
    contract_id          TEXT    NOT NULL,   -- C…
    durability           TEXT    NOT NULL CHECK (durability IN ('temporary', 'persistent')),
    key_xdr              BYTEA   NOT NULL,   -- SCVal
    val_xdr              BYTEA   NOT NULL,   -- SCVal
    key                  JSONB   NOT NULL,
    val                  JSONB   NOT NULL,
    balance_holder       TEXT,               -- SAC balance entries (ingest/sac)
    balance              NUMERIC,
    executable           TEXT    CHECK (executable IN ('wasm', 'stellar_asset')), -- instance entries
    wasm_hash            BYTEA,              -- instance entries of Wasm contracts
    asset                TEXT,               -- instance entries of SACs: native or CODE:ISSUER
    archived             BOOLEAN NOT NULL DEFAULT false,
    last_modified_ledger BIGINT  NOT NULL
);
CREATE INDEX contract_data_contract_idx ON explorer.contract_data (contract_id);
CREATE INDEX contract_data_balance_holder_idx ON explorer.contract_data (balance_holder) WHERE balance_holder IS NOT NULL;
CREATE UNIQUE INDEX contract_data_instance_idx ON explorer.contract_data (contract_id) WHERE executable IS NOT NULL;
CREATE INDEX contract_data_wasm_hash_idx ON explorer.contract_data (wasm_hash) WHERE wasm_hash IS NOT NULL;
CREATE INDEX contract_data_asset_idx ON explorer.contract_data (asset) WHERE asset IS NOT NULL;
