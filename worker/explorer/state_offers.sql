-- offers: the DEX's open offers, current state. price is buying per unit of
-- selling (price_n / price_d), computed by PostgreSQL; amount is in stroops
-- of selling. offers_pair_idx serves the order book of a pair
-- (api.order_book), offers_seller_idx an account's offers.

-- +migration 10 state_offers
CREATE TABLE explorer.offers (
    offer_id             BIGINT  PRIMARY KEY,
    seller_id            TEXT    NOT NULL,   -- G…
    selling              TEXT    NOT NULL,   -- native or CODE:ISSUER
    buying               TEXT    NOT NULL,
    amount               BIGINT  NOT NULL,
    price_n              INTEGER NOT NULL,
    price_d              INTEGER NOT NULL,
    price                NUMERIC GENERATED ALWAYS AS (price_n::numeric / price_d) STORED,
    flags                INTEGER NOT NULL,   -- 1: passive
    sponsor              TEXT,
    last_modified_ledger BIGINT  NOT NULL
);
CREATE INDEX offers_seller_idx ON explorer.offers (seller_id, offer_id);
CREATE INDEX offers_pair_idx ON explorer.offers (selling, buying, price, offer_id);
