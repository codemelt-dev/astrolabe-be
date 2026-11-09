-- offers: the DEX's open offers in the daily state copy (state_offers, from PostgreSQL's
-- explorer.offers, replaced by each run like the other state_* tables), and how much of each
-- asset is on offer (on_offers: the sum of the accounts' selling liabilities). on_offers lands
-- in asset_snapshots every day; snapshots taken before this migration have NULL there.
-- +migration 15 offers
CREATE TABLE IF NOT EXISTS state_offers
(
    offer_id             Int64,
    seller               String,         -- G…
    selling              String,         -- native or CODE:ISSUER
    buying               String,
    amount               Int64,          -- of selling, base units
    price_n              UInt32,
    price_d              UInt32,
    price                Float64 MATERIALIZED price_n / price_d, -- buying per unit of selling
    flags                UInt32,         -- 1: passive
    last_modified_ledger UInt32
)
ENGINE = MergeTree
ORDER BY (selling, buying, price, offer_id);

ALTER TABLE assets ADD COLUMN IF NOT EXISTS on_offers Int128 AFTER in_contracts;
ALTER TABLE asset_snapshots ADD COLUMN IF NOT EXISTS on_offers Nullable(Int128) AFTER in_contracts;
