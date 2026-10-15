-- liquidity_pools: the AMM pools (constant product), current state. Pool
-- shares are trustlines whose asset is the pool id in hex.

-- +migration 12 state_liquidity_pools
CREATE TABLE explorer.liquidity_pools (
    pool_id              BYTEA   PRIMARY KEY,
    asset_a              TEXT    NOT NULL,    -- native or CODE:ISSUER
    asset_b              TEXT    NOT NULL,
    fee_bp               INTEGER NOT NULL,    -- basis points
    reserve_a            BIGINT  NOT NULL,
    reserve_b            BIGINT  NOT NULL,
    total_shares         BIGINT  NOT NULL,
    trustlines           BIGINT  NOT NULL,    -- pool-share trustlines
    last_modified_ledger BIGINT  NOT NULL
);
CREATE INDEX liquidity_pools_assets_idx ON explorer.liquidity_pools (asset_a, asset_b);
