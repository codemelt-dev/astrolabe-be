-- ingest_state: one cursor per pipeline. Every commit advances
-- it in the same transaction as the rows, guarded by the previous sequence
-- and hash. `reindex:<A>-<B>` rows track a running or finished reindex.
-- It lives in schema public (the pre-go-live reset in psql/README.md drops
-- public.ingest_state).

-- +migration 1 ingest_state
CREATE TABLE public.ingest_state (
    pipeline        TEXT PRIMARY KEY,
    ledger_sequence BIGINT NOT NULL,
    ledger_hash     TEXT   NOT NULL,   -- lower-case hex of the ledger hash
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
