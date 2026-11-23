-- ledger_hash: a ledger by its hash. ledgers is ordered by sequence, so without an index a lookup
-- by hash reads every ledger. The bloom filter (1 false positive in 1,000) leaves a few granules.
-- MATERIALIZE INDEX builds it for the ledgers already written, in the background.
-- +migration 18 ledger_hash
ALTER TABLE ledgers ADD INDEX IF NOT EXISTS hash_bf hash TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE ledgers MATERIALIZE INDEX hash_bf;
