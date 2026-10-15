-- state_checkpoint: the history-archive checkpoint the current-state tables
-- were loaded from. One row, written by `worker bootstrap` (and
-- replaced by `worker bootstrap --force`) in the same transaction as the
-- state load and the cursor. Without it (after `bootstrap --history-only`)
-- the state tables are empty and ingest leaves them alone.

-- +migration 8 state_checkpoint
CREATE TABLE explorer.state_checkpoint (
    singleton    BOOLEAN     PRIMARY KEY DEFAULT true CHECK (singleton),
    checkpoint   BIGINT      NOT NULL,   -- C, with (C + 1) % 64 = 0
    hash         BYTEA       NOT NULL,   -- ledger C's hash
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    load_seconds DOUBLE PRECISION NOT NULL, -- bootstrap duration (PLAN M5)
    entries      JSONB       NOT NULL    -- rows loaded per state table
);
