-- ttl: current state. The API derives "expired" from live_until_ledger.

-- +migration 7 state_ttl
CREATE TABLE explorer.ttl (
    key_hash             BYTEA  PRIMARY KEY, -- = TtlEntry.KeyHash
    live_until_ledger    BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL
);
