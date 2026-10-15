-- tx_by_hash: a transaction by its hash. The key is the first
-- 8 bytes of the hash, and of a fee bump's inner hash: 11.8 bytes a row
-- against 36.3 for the whole hash, and as fast. A lookup reads a few
-- granules, then checks the whole hash against transactions at the ledger
-- and index it found, so a hash we don't have returns nothing and two that
-- share 8 bytes are told apart. The rows are random in the key: the ledger
-- column's minmax index lets the delete above the cursor skip old parts.

-- +migration 7 tx_by_hash
CREATE TABLE IF NOT EXISTS tx_by_hash
(
    prefix          FixedString(8),         -- substring(hash, 1, 8)
    ledger_sequence UInt32,
    tx_index        UInt32,
    INDEX ledger_mm ledger_sequence TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(ledger_sequence, 1000000)
ORDER BY prefix;
