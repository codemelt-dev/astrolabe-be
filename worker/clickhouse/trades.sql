-- trades: one row per fill of the DEX (explorer.TradeRow): an offer an operation matched in the
-- order book, or a liquidity pool it swapped with, from the operation's result. The seller is the
-- offer's owner, or the pool (L…); the buyer is the operation's source. The price, from the
-- seller's side, is bought_amount / sold_amount. Stored like the other history tables: the newest
-- month on the local disk, the rest on the data volume (storage.sql).
-- +migration 16 trades
CREATE TABLE IF NOT EXISTS trades
(
    ledger_sequence UInt32,
    closed_at       DateTime('UTC'),
    tx_index        UInt32,                 -- 1-based
    op_index        UInt32,                 -- 0-based
    position        UInt32,                 -- the fill's index in the operation's result
    venue           LowCardinality(String), -- orderbook or pool
    offer_id        Int64,                  -- the matched offer; 0 for a pool
    seller          String,                 -- G…, or the pool (L…)
    buyer           String,                 -- G…
    sold_asset      String,                 -- what the seller gave: native or CODE:ISSUER
    sold_amount     Int64,                  -- base units
    bought_asset    String,                 -- what the seller received
    bought_amount   Int64,
    INDEX sold_bf sold_asset TYPE bloom_filter GRANULARITY 4,
    INDEX bought_bf bought_asset TYPE bloom_filter GRANULARITY 4,
    INDEX seller_bf seller TYPE bloom_filter GRANULARITY 4,
    INDEX buyer_bf buyer TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(closed_at)
ORDER BY (ledger_sequence, tx_index, op_index, position);

ALTER TABLE trades MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE trades MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
