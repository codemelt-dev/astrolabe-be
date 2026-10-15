-- daily_stats: one row per UTC day. The refreshable materialized view
-- recomputes, every 5 minutes, the two newest days the pipeline has reached
-- (by the newest ledger's close time, so a re-ingest computes every day as
-- it passes through) and appends them; ReplacingMergeTree keeps the latest
-- computation of a day: read it with FINAL.

-- +migration 11 daily_stats
CREATE TABLE IF NOT EXISTS daily_stats
(
    day                   Date,
    ledgers               UInt64,
    transactions          UInt64,
    failed_transactions   UInt64,
    soroban_transactions  UInt64,
    fee_bump_transactions UInt64,
    operations            UInt64,
    successful_operations UInt64,
    operations_by_type    Map(LowCardinality(String), UInt64),
    active_accounts       UInt64,                 -- distinct transaction sources
    fees_charged          Int64,                  -- stroops
    new_accounts          UInt64,                 -- successful create_account operations
    new_contracts         UInt64,
    contract_calls        UInt64,
    token_transfers       UInt64,
    computed_at           DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_mv
REFRESH EVERY 5 MINUTE APPEND TO daily_stats AS
WITH (SELECT toDate(max(closed_at)) - 1 FROM ledgers) AS since
SELECT l.day AS day, l.ledgers, l.transactions, l.failed_transactions, t.soroban_transactions, t.fee_bump_transactions,
       l.operations, l.successful_operations, o.operations_by_type, t.active_accounts, t.fees_charged, o.new_accounts,
       c.new_contracts, k.contract_calls, x.token_transfers, now() AS computed_at
FROM (SELECT toDate(closed_at) AS day, count() AS ledgers, sum(tx_count) AS transactions, sum(failed_tx_count) AS failed_transactions,
             sum(op_count) AS operations, sum(successful_op_count) AS successful_operations
      FROM ledgers WHERE toDate(closed_at) >= since GROUP BY day) AS l
LEFT JOIN (SELECT toDate(closed_at) AS day, countIf(soroban) AS soroban_transactions, countIf(notEmpty(inner_hash)) AS fee_bump_transactions,
                  uniqExact(source_account) AS active_accounts, sum(fee_charged) AS fees_charged
           FROM transactions WHERE toDate(closed_at) >= since GROUP BY day) AS t USING day
LEFT JOIN (SELECT day, mapFromArrays(groupArray(type_name), groupArray(n)) AS operations_by_type,
                  sumIf(ok, type_name = 'create_account') AS new_accounts
           FROM (SELECT toDate(closed_at) AS day, type_name, count() AS n, countIf(successful) AS ok
                 FROM operations WHERE toDate(closed_at) >= since GROUP BY day, type_name)
           GROUP BY day) AS o USING day
LEFT JOIN (SELECT toDate(closed_at) AS day, count() AS new_contracts FROM contract_creations
           WHERE toDate(closed_at) >= since GROUP BY day) AS c USING day
LEFT JOIN (SELECT toDate(closed_at) AS day, count() AS contract_calls FROM contract_calls
           WHERE toDate(closed_at) >= since GROUP BY day) AS k USING day
LEFT JOIN (SELECT toDate(closed_at) AS day, count() AS token_transfers FROM token_transfers
           WHERE toDate(closed_at) >= since AND kind != 'fee' GROUP BY day) AS x USING day;
