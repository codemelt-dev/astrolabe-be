-- accounts: current state. Loaded by `worker bootstrap` from a checkpoint
-- and maintained per ledger, like every state table.

-- +migration 3 state_accounts
CREATE TABLE explorer.accounts (
    account_id           TEXT     PRIMARY KEY, -- G…
    balance              BIGINT   NOT NULL,
    seq_num              BIGINT   NOT NULL,
    seq_ledger           BIGINT,              -- AccountEntryExtensionV3
    seq_time             BIGINT,
    num_subentries       INTEGER  NOT NULL,
    num_sponsored        INTEGER  NOT NULL DEFAULT 0,
    num_sponsoring       INTEGER  NOT NULL DEFAULT 0,
    inflation_dest       TEXT,
    flags                INTEGER  NOT NULL,
    home_domain          TEXT     NOT NULL,
    master_weight        SMALLINT NOT NULL,
    threshold_low        SMALLINT NOT NULL,
    threshold_medium     SMALLINT NOT NULL,
    threshold_high       SMALLINT NOT NULL,
    signers              JSONB    NOT NULL,   -- [{"key": "G…|T…|X…|P…", "weight": n, "sponsor": "G…"|null}]
    buying_liabilities   BIGINT   NOT NULL DEFAULT 0,
    selling_liabilities  BIGINT   NOT NULL DEFAULT 0,
    sponsor              TEXT,                -- sponsor of the entry
    last_modified_ledger BIGINT   NOT NULL
);
