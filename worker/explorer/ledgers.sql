-- ledgers: one row per ledger, the only history PostgreSQL keeps (the rest
-- is the clickhouse pipeline's): api.ledgers and api.status read it, `worker
-- bootstrap --recreate-origin` reads its first row, and reindex checks
-- hashes against it. Small (17,280 rows, ~6.4 MB a day), so not partitioned.

-- +migration 2 ledgers
CREATE TABLE explorer.ledgers (
    sequence            BIGINT      PRIMARY KEY,
    hash                BYTEA       NOT NULL,
    prev_hash           BYTEA       NOT NULL,
    closed_at           TIMESTAMPTZ NOT NULL,
    protocol_version    INTEGER     NOT NULL,
    tx_count            INTEGER     NOT NULL,  -- transactions in the set, failed included
    failed_tx_count     INTEGER     NOT NULL,
    op_count            INTEGER     NOT NULL,  -- operations of all transactions
    successful_op_count INTEGER     NOT NULL,  -- operations of successful transactions
    base_fee            INTEGER     NOT NULL,
    base_reserve        INTEGER     NOT NULL,
    max_tx_set_size     INTEGER     NOT NULL,
    total_coins         BIGINT      NOT NULL,
    fee_pool            BIGINT      NOT NULL,
    soroban_state_size  BIGINT,                -- total live Soroban state, bytes
    source              TEXT        NOT NULL CHECK (source IN ('galexie', 'aws_lake')),
    core_version        TEXT                   -- producer's x-amz-meta-core-version, when read
);
CREATE UNIQUE INDEX ledgers_hash_idx ON explorer.ledgers (hash);
