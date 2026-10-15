-- grants.sql: per-object grants on schema explorer, applied after `worker
-- migrate` created it. The default privileges of roles.sql cover every new
-- table for ingest_writer; this file catches up the ones created before
-- them, and gives state_reader what `worker state-copy` reads.
--
-- lab psql (step api) runs it as explorer_owner, the owner of every object
-- here, right before api.sql: after the first migration and again after every
-- later one (README §7.2). It is idempotent. By hand:
--   (echo 'SET ROLE explorer_owner;'; cat grants.sql) |
--     runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d explorer -f -

SET client_min_messages = warning;

-- `worker bootstrap` truncates the current-state tables. Tables created before
-- the TRUNCATE default privilege existed get it here.
GRANT TRUNCATE ON ALL TABLES IN SCHEMA explorer TO ingest_writer;

-- worker state-copy: the explorer cursor, its ledger, and the state it copies.
GRANT USAGE ON SCHEMA explorer TO state_reader;
GRANT SELECT ON public.ingest_state, explorer.ledgers, explorer.accounts, explorer.trustlines, explorer.contract_data,
    explorer.claimable_balances, explorer.liquidity_pools TO state_reader;
