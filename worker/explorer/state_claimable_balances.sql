-- claimable_balances: balances waiting for a claimant, current state.
-- claimants is [{"destination": "G…", "predicate": …}], the predicate as the
-- XDR's JSON; the GIN index finds an account's balances with
-- claimants @> '[{"destination": "G…"}]'.

-- +migration 11 state_claimable_balances
CREATE TABLE explorer.claimable_balances (
    balance_id           TEXT    PRIMARY KEY, -- B…
    asset                TEXT    NOT NULL,    -- native or CODE:ISSUER
    amount               BIGINT  NOT NULL,
    claimants            JSONB   NOT NULL,
    flags                INTEGER NOT NULL,    -- 1: clawback enabled
    sponsor              TEXT,
    last_modified_ledger BIGINT  NOT NULL
);
CREATE INDEX claimable_balances_claimants_idx ON explorer.claimable_balances USING gin (claimants jsonb_path_ops);
CREATE INDEX claimable_balances_asset_idx ON explorer.claimable_balances (asset);
