-- data_entries: each account's name/value pairs (ManageData), current state.

-- +migration 9 state_data_entries
CREATE TABLE explorer.data_entries (
    account_id           TEXT   NOT NULL,   -- G…
    name                 TEXT   NOT NULL,   -- at most 64 bytes
    value                BYTEA  NOT NULL,   -- at most 64 bytes
    sponsor              TEXT,
    last_modified_ledger BIGINT NOT NULL,
    PRIMARY KEY (account_id, name)
);
