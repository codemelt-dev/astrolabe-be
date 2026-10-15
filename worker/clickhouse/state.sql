-- state_*: a copy of PostgreSQL's current state, for the rankings
-- PostgreSQL is slow at. `worker state-copy` writes it from
-- the psql host once a day (worker/statecopy), as stellar_jobs: it fills a
-- <name>_new table for each, then swaps it in with EXCHANGE TABLES, so a
-- reader sees one copy or the next, never half of one. Ordered for the
-- rankings that read them. state_copy_runs records each run.

-- +migration 12 state_copy
CREATE TABLE IF NOT EXISTS state_accounts
(
    account_id           String,
    balance              Int64,          -- stroops
    buying_liabilities   Int64,
    selling_liabilities  Int64,
    num_subentries       UInt32,
    flags                UInt32,
    home_domain          String,
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY (balance, account_id);

CREATE TABLE IF NOT EXISTS state_trustlines
(
    asset                String,         -- CODE:ISSUER; pool shares are not copied
    account_id           String,
    balance              Int64,
    trust_limit          Int64,
    flags                UInt32,
    buying_liabilities   Int64,
    selling_liabilities  Int64,
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY (asset, balance, account_id);

CREATE TABLE IF NOT EXISTS state_sac_balances
(
    asset                String,         -- native or CODE:ISSUER
    contract_id          String,         -- the Stellar Asset Contract
    holder               String,         -- C… (accounts hold an asset through trustlines)
    balance              Int128,
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY (asset, balance, holder);

CREATE TABLE IF NOT EXISTS state_asset_contracts
(
    asset                String,         -- native or CODE:ISSUER
    contract_id          String          -- its Stellar Asset Contract
)
ENGINE = MergeTree
ORDER BY asset;

CREATE TABLE IF NOT EXISTS state_claimable_balances
(
    asset                String,
    balance_id           String,         -- B…
    amount               Int64,
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY (asset, balance_id);

CREATE TABLE IF NOT EXISTS state_liquidity_pools
(
    pool_id              String,         -- hex
    asset_a              String,
    asset_b              String,
    reserve_a            Int64,
    reserve_b            Int64,
    total_shares         Int64,
    trustlines           UInt64,
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY pool_id;

CREATE TABLE IF NOT EXISTS state_copy_runs
(
    started_at  DateTime('UTC'),
    finished_at DateTime('UTC'),
    ledger      UInt32,                  -- the explorer cursor the copy is as of
    closed_at   DateTime('UTC'),         -- and when that ledger closed
    rows        Map(LowCardinality(String), UInt64),
    snapshot    Date                     -- the snapshots' date, or 1970-01-01 without one
)
ENGINE = MergeTree
ORDER BY started_at;
