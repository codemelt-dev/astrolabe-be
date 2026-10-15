-- trustlines: current state.

-- +migration 4 state_trustlines
CREATE TABLE explorer.trustlines (
    account_id           TEXT    NOT NULL,   -- G…
    asset                TEXT    NOT NULL,   -- 'CODE:ISSUER', or the pool id (hex) for pool shares
    liquidity_pool_id    BYTEA,              -- pool-share trustlines
    balance              BIGINT  NOT NULL,
    trust_limit          BIGINT  NOT NULL,
    flags                INTEGER NOT NULL,
    buying_liabilities   BIGINT  NOT NULL DEFAULT 0,
    selling_liabilities  BIGINT  NOT NULL DEFAULT 0,
    sponsor              TEXT,
    last_modified_ledger BIGINT  NOT NULL,
    PRIMARY KEY (account_id, asset)
);
CREATE INDEX trustlines_asset_idx ON explorer.trustlines (asset);
