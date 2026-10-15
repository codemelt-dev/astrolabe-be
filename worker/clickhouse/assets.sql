-- assets and top_holders: computed by `worker state-copy` from the state
-- copy (state.sql) and replaced with it; asset_snapshots and
-- holder_snapshots keep them by date, from the first daily run on.
--
-- An asset is native, CODE:ISSUER, or a Soroban-only token's contract (C…,
-- a SEP-41 contract without a Stellar Asset Contract). A classic asset's
-- supply is what its trustlines, claimable balances, pool reserves and
-- contracts (SAC balances) hold; XLM's is the ledger's total_coins. A
-- Soroban-only token's supply and holders are summed from token_transfers,
-- so they are complete (complete = true) only for tokens created since
-- go-live. top_holders has the top 500 of each asset and the top 1,000 of
-- XLM, accounts and contracts alike.

-- +migration 13 assets
CREATE TABLE IF NOT EXISTS assets
(
    asset                 String,
    kind                  LowCardinality(String), -- native, classic or soroban
    code                  String,
    issuer                String,
    contract_id           String,                 -- the Stellar Asset Contract, or the token contract; '' if none
    holders               UInt64,                 -- accounts and contracts with a positive balance
    trustlines            UInt64,                 -- XLM: accounts
    supply                Int128,                 -- base units (stroops for classic assets)
    in_accounts           Int128,                 -- trustlines; XLM: account balances
    in_claimable_balances Int128,
    in_liquidity_pools    Int128,
    in_contracts          Int128,                 -- Stellar Asset Contract balances of contracts
    issuer_flags          UInt32,
    home_domain           String,
    complete              Bool,
    ledger                UInt32,                 -- the state copy's ledger
    computed_at           DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY asset;

CREATE TABLE IF NOT EXISTS top_holders
(
    asset   String,
    rank    UInt16,                               -- 1 is the largest
    holder  String,                               -- G… or C…
    balance Int128,
    ledger  UInt32
)
ENGINE = MergeTree
ORDER BY (asset, rank);

CREATE TABLE IF NOT EXISTS asset_snapshots
(
    date                  Date,
    asset                 String,
    kind                  LowCardinality(String),
    code                  String,
    issuer                String,
    contract_id           String,
    holders               UInt64,
    trustlines            UInt64,
    supply                Int128,
    in_accounts           Int128,
    in_claimable_balances Int128,
    in_liquidity_pools    Int128,
    in_contracts          Int128,
    issuer_flags          UInt32,
    home_domain           String,
    complete              Bool,
    ledger                UInt32,
    computed_at           DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (asset, date);

CREATE TABLE IF NOT EXISTS holder_snapshots
(
    date    Date,
    asset   String,
    rank    UInt16,
    holder  String,
    balance Int128,
    ledger  UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (asset, date, rank);
