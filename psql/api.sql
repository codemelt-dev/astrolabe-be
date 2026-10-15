-- api.sql: the explorer's public API, served by PostgREST.
--
-- API version 2 (2026-10-09), documented in README §7: current state and the
-- ledgers. The history (transactions, operations, events) is ClickHouse's
-- (clickhouse/README.md). Every object here belongs to the API;
-- the worker owns schema explorer and public.ingest_state (its migrations,
-- which `worker migrate` applies from the galexie host).
--
-- What it creates, all owned by explorer_owner:
--   part 1  supporting indexes on explorer.* (created only when missing);
--   part 2  schema api_types: domains that set how hashes, XDR and 64-bit
--           integers look in JSON and in filters (PostgREST "domain
--           representations"), and their cast functions;
--           schema api: read-only views over explorer.*, the functions that
--           PostgREST embeds as computed relationships, and the grants to the
--           anonymous role (web_anon).
--
-- lab psql (step api) applies it on this host as explorer_owner, after
-- grants.sql, once `worker migrate` has created the schema version checked
-- below (README §7.2). By hand:
--   (echo 'SET ROLE explorer_owner;'; cat api.sql) |
--     runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d explorer -f -
-- It is safe to re-run. Part 2 replaces both schemas in one transaction and
-- ends with NOTIFY pgrst, so PostgREST reloads its schema cache on commit.
--
-- Plain SQL, no psql meta-commands, so any client can run it. The anonymous
-- role is web_anon unless the session sets
--   SET stellar.api_anon_role = '<role>';
-- before running this file (scratch databases with prefixed roles).

------------------------------------------------------------------------------
-- Part 1: supporting indexes
--
-- Each one serves a documented query pattern (README §7.3, §7.4). Creating
-- one blocks ingest writes to that table until it is built: on a populated
-- database, build new indexes first with the procedure in README §7.6, then
-- this part only finds them. The state tables' own lookups (an account's
-- offers, an order book, a claimant's balances, the contracts) are indexed by
-- their migrations.
------------------------------------------------------------------------------

BEGIN;
SET LOCAL search_path = pg_catalog;
SET LOCAL statement_timeout = 0;
SET LOCAL lock_timeout = '30s';
SET LOCAL client_min_messages = warning;   -- not "already exists, skipping"

DO $$
DECLARE
    owner name;
BEGIN
    IF to_regclass('explorer.schema_migrations') IS NULL THEN
        RAISE EXCEPTION 'api.sql: schema explorer does not exist; run `worker migrate` first';
    END IF;
    -- Migration 12 (state_liquidity_pools) is the newest one the views below
    -- read.
    IF NOT EXISTS (SELECT 1 FROM explorer.schema_migrations WHERE version >= 12) THEN
        RAISE EXCEPTION 'api.sql: schema explorer is older than migration 12; run `worker migrate` first';
    END IF;
    SELECT pg_get_userbyid(relowner) INTO owner FROM pg_class WHERE oid = 'explorer.ledgers'::regclass;
    IF owner <> current_user THEN
        RAISE EXCEPTION 'api.sql: run as %, the owner of schema explorer, not as % (SET ROLE %)',
            owner, current_user, owner;
    END IF;
END
$$;

-- Ledgers by close time (find the ledger range of a date).
CREATE INDEX IF NOT EXISTS api_ledgers_closed_at_idx
    ON explorer.ledgers (closed_at);

-- An asset's trustlines, paged by account.
CREATE INDEX IF NOT EXISTS api_trustlines_asset_account_idx
    ON explorer.trustlines (asset, account_id);

-- A contract's data entries, paged by key hash.
CREATE INDEX IF NOT EXISTS api_contract_data_contract_key_idx
    ON explorer.contract_data (contract_id, key_hash);

-- Contract code by Wasm hash (a contract instance refers to its code by hash).
CREATE INDEX IF NOT EXISTS api_contract_code_code_hash_idx
    ON explorer.contract_code (code_hash);

COMMIT;

------------------------------------------------------------------------------
-- Part 2: schemas api_types and api, replaced as a whole
------------------------------------------------------------------------------

BEGIN;
SET LOCAL search_path = pg_catalog;
-- DROP SCHEMA waits for running API queries (at most statement_timeout, 2 s,
-- each) and blocks new ones meanwhile; on a timeout just run the file again.
SET LOCAL lock_timeout = '5s';
SET LOCAL client_min_messages = warning;   -- not the list of what CASCADE drops

DROP SCHEMA IF EXISTS api CASCADE;
DROP SCHEMA IF EXISTS api_types CASCADE;

CREATE SCHEMA api_types;
COMMENT ON SCHEMA api_types IS
    'Representation domains of the explorer API (api.sql); not exposed by PostgREST.';

-- Domain representations. A view column cast to one of these domains keeps
-- its base type for filtering, ordering and indexes; PostgREST calls the
-- domain's cast to json for the response and, if there is one, the cast from
-- text to parse filter values. The domains have no constraints, so the
-- planner reduces the casts to no-ops and indexes on the base columns apply.

-- Hashes and keys (32 bytes and others): lower-case hex; filters accept hex.
CREATE DOMAIN api_types.hex AS bytea;
CREATE FUNCTION api_types.hex_json(api_types.hex) RETURNS json
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT to_json(encode($1, 'hex')) $$;
CREATE FUNCTION api_types.hex_parse(text) RETURNS api_types.hex
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT decode($1, 'hex')::api_types.hex $$;

-- XDR: base64, as Horizon and RPC return it. Not meant for filtering.
CREATE DOMAIN api_types.base64 AS bytea;
CREATE FUNCTION api_types.base64_json(api_types.base64) RETURNS json
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT to_json(translate(encode($1, 'base64'), E'\n', '')) $$;

-- 64-bit integers that can exceed 2^53 (stroop amounts, sequence numbers,
-- TOIDs): JSON strings, so JavaScript clients don't lose precision.
-- Filters take plain decimal numbers.
CREATE DOMAIN api_types.int64 AS bigint;
CREATE FUNCTION api_types.int64_json(api_types.int64) RETURNS json
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT to_json($1::text) $$;

-- Token amounts (up to i128/u256 base units): JSON strings.
CREATE DOMAIN api_types.amount AS numeric;
CREATE FUNCTION api_types.amount_json(api_types.amount) RETURNS json
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT to_json($1::text) $$;

-- PostgreSQL warns that it ignores casts on domains; PostgREST reads them.
SET LOCAL client_min_messages = error;
CREATE CAST (api_types.hex AS json) WITH FUNCTION api_types.hex_json(api_types.hex) AS IMPLICIT;
CREATE CAST (text AS api_types.hex) WITH FUNCTION api_types.hex_parse(text) AS IMPLICIT;
CREATE CAST (api_types.base64 AS json) WITH FUNCTION api_types.base64_json(api_types.base64) AS IMPLICIT;
CREATE CAST (api_types.int64 AS json) WITH FUNCTION api_types.int64_json(api_types.int64) AS IMPLICIT;
CREATE CAST (api_types.amount AS json) WITH FUNCTION api_types.amount_json(api_types.amount) AS IMPLICIT;
SET LOCAL client_min_messages = warning;

CREATE SCHEMA api;
COMMENT ON SCHEMA api IS $$Stellar explorer API
Read-only pubnet current state (accounts, trustlines, data entries, offers, claimable balances, liquidity pools, contracts, their data and code) and the ledgers since the state was loaded. API version 2. Transactions, operations, events and statistics are in ClickHouse, served as public read-only SQL. Hashes are hex, XDR is base64, 64-bit integers and token amounts are strings. Page with keyset filters on the primary key columns, not with offset.$$;

-- Ledgers ------------------------------------------------------------------

CREATE VIEW api.ledgers AS
SELECT l.sequence,
       l.hash::api_types.hex      AS hash,
       l.prev_hash::api_types.hex AS prev_hash,
       l.closed_at,
       l.protocol_version,
       l.tx_count,
       l.failed_tx_count,
       l.op_count,
       l.successful_op_count,
       l.base_fee,
       l.base_reserve,
       l.max_tx_set_size,
       l.total_coins::api_types.int64 AS total_coins,
       l.fee_pool::api_types.int64    AS fee_pool,
       l.soroban_state_size,
       l.core_version
  FROM explorer.ledgers l;
COMMENT ON VIEW api.ledgers IS $$Ledgers, from the state's checkpoint on
Key: sequence. Lookups: sequence, hash, closed_at (range). Older ledgers, and their transactions, are in ClickHouse.$$;

-- Current state ------------------------------------------------------------

CREATE VIEW api.accounts AS
SELECT a.account_id,
       a.balance::api_types.int64 AS balance,
       a.seq_num::api_types.int64 AS seq_num,
       a.seq_ledger,
       a.seq_time::api_types.int64 AS seq_time,
       a.num_subentries,
       a.num_sponsored,
       a.num_sponsoring,
       a.inflation_dest,
       a.flags,
       a.home_domain,
       a.master_weight,
       a.threshold_low,
       a.threshold_medium,
       a.threshold_high,
       a.signers,
       a.buying_liabilities::api_types.int64  AS buying_liabilities,
       a.selling_liabilities::api_types.int64 AS selling_liabilities,
       a.sponsor,
       a.last_modified_ledger
  FROM explorer.accounts a;
COMMENT ON VIEW api.accounts IS $$Accounts (current state)
Key: account_id. Balances are in stroops.$$;

CREATE VIEW api.trustlines AS
SELECT tl.account_id,
       tl.asset,
       tl.liquidity_pool_id::api_types.hex AS liquidity_pool_id,
       tl.balance::api_types.int64     AS balance,
       tl.trust_limit::api_types.int64 AS trust_limit,
       tl.flags,
       tl.buying_liabilities::api_types.int64  AS buying_liabilities,
       tl.selling_liabilities::api_types.int64 AS selling_liabilities,
       tl.sponsor,
       tl.last_modified_ledger
  FROM explorer.trustlines tl;
COMMENT ON VIEW api.trustlines IS $$Trustlines (current state)
Key: (account_id, asset); asset is CODE:ISSUER, or the pool id for pool shares. Lookups: an account's trustlines, an asset's trustlines (by account_id).$$;

-- live_until_ledger and expired come from the TTL entry; expired is relative
-- to the newest ingested ledger. archived means evicted after go-live.
CREATE VIEW api.contract_data AS
SELECT d.key_hash::api_types.hex AS key_hash,
       d.contract_id,
       d.durability,
       d.key,
       d.val,
       d.balance_holder,
       d.balance::api_types.amount AS balance,
       d.archived,
       x.live_until_ledger,
       x.live_until_ledger < (SELECT s.ledger_sequence FROM public.ingest_state s WHERE s.pipeline = 'explorer') AS expired,
       d.last_modified_ledger,
       d.key_xdr::api_types.base64 AS key_xdr,
       d.val_xdr::api_types.base64 AS val_xdr
  FROM explorer.contract_data d
  LEFT JOIN explorer.ttl x ON x.key_hash = d.key_hash;
COMMENT ON VIEW api.contract_data IS $$Contract data entries (current state)
Key: key_hash = sha256 of the LedgerKey XDR. Lookups: key_hash, a contract's entries (by key_hash). SAC balance entries are also in sac_balances.$$;

CREATE VIEW api.sac_balances AS
SELECT * FROM api.contract_data WHERE balance_holder IS NOT NULL;
COMMENT ON VIEW api.sac_balances IS $$Stellar Asset Contract balance entries (contract data with a balance holder)
Key: key_hash. Lookups: balance_holder (an account's or contract's balances), contract_id + key_hash.$$;

-- A contract's instance entry, one per contract.
CREATE VIEW api.contracts AS
SELECT d.contract_id,
       d.executable,
       d.wasm_hash::api_types.hex AS wasm_hash,
       d.asset,
       d.archived,
       x.live_until_ledger,
       x.live_until_ledger < (SELECT s.ledger_sequence FROM public.ingest_state s WHERE s.pipeline = 'explorer') AS expired,
       d.last_modified_ledger
  FROM explorer.contract_data d
  LEFT JOIN explorer.ttl x ON x.key_hash = d.key_hash
 WHERE d.executable IS NOT NULL;
COMMENT ON VIEW api.contracts IS $$Contracts (current state)
Key: contract_id. executable is wasm or stellar_asset; asset is a Stellar Asset Contract's (native or CODE:ISSUER). Lookups: contract_id, wasm_hash (the contracts running a code), asset. Embed the code and its functions with select=*,contract_code(spec). Who created a contract, and when: ClickHouse's contract_creations.$$;

CREATE VIEW api.contract_code AS
SELECT c.key_hash::api_types.hex  AS key_hash,
       c.code_hash::api_types.hex AS code_hash,
       c.wasm_size,
       c.spec,
       c.archived,
       x.live_until_ledger,
       x.live_until_ledger < (SELECT s.ledger_sequence FROM public.ingest_state s WHERE s.pipeline = 'explorer') AS expired,
       c.last_modified_ledger
  FROM explorer.contract_code c
  LEFT JOIN explorer.ttl x ON x.key_hash = c.key_hash;
COMMENT ON VIEW api.contract_code IS $$Contract code entries (current state)
Key: key_hash. Lookups: code_hash. spec is the Wasm's contractspecv0 section: its functions with their argument and result types, and the types, events and errors they use (null when the code has none). The Wasm itself is in contract_wasm.$$;

CREATE VIEW api.contract_wasm AS
SELECT c.code_hash::api_types.hex AS code_hash,
       c.wasm::api_types.base64   AS wasm,
       c.wasm_size
  FROM explorer.contract_code c;
COMMENT ON VIEW api.contract_wasm IS $$Uploaded Wasm (current state; archived code included)
Key: code_hash. The code as base64, up to 128 KB a row: select one at a time.$$;

CREATE VIEW api.data_entries AS
SELECT d.account_id,
       d.name,
       d.value::api_types.base64 AS value,
       d.sponsor,
       d.last_modified_ledger
  FROM explorer.data_entries d;
COMMENT ON VIEW api.data_entries IS $$Data entries: an account's name/value pairs (current state)
Key: (account_id, name). value is base64 (up to 64 bytes).$$;

CREATE VIEW api.offers AS
SELECT o.offer_id::api_types.int64 AS offer_id,
       o.seller_id,
       o.selling,
       o.buying,
       o.amount::api_types.int64 AS amount,
       o.price_n,
       o.price_d,
       trim_scale(o.price)::api_types.amount AS price,
       o.flags,
       o.sponsor,
       o.last_modified_ledger
  FROM explorer.offers o;
COMMENT ON VIEW api.offers IS $$The DEX's open offers (current state)
Key: offer_id. amount is in stroops of selling; price is buying per unit of selling (price_n / price_d); flags 1 is passive. Lookups: seller_id (by offer_id), a pair's offers (selling + buying, by price). The aggregated order book is rpc/order_book.$$;

CREATE VIEW api.claimable_balances AS
SELECT c.balance_id,
       c.asset,
       c.amount::api_types.int64 AS amount,
       c.claimants,
       c.flags,
       c.sponsor,
       c.last_modified_ledger
  FROM explorer.claimable_balances c;
COMMENT ON VIEW api.claimable_balances IS $$Claimable balances (current state)
Key: balance_id (B…). claimants is [{"destination", "predicate"}]. Lookups: balance_id, asset, an account's: claimants=cs.[{"destination":"G…"}].$$;

CREATE VIEW api.liquidity_pools AS
SELECT p.pool_id::api_types.hex AS pool_id,
       p.asset_a,
       p.asset_b,
       p.fee_bp,
       p.reserve_a::api_types.int64    AS reserve_a,
       p.reserve_b::api_types.int64    AS reserve_b,
       p.total_shares::api_types.int64 AS total_shares,
       p.trustlines,
       p.last_modified_ledger
  FROM explorer.liquidity_pools p;
COMMENT ON VIEW api.liquidity_pools IS $$Liquidity pools (current state)
Key: pool_id; its shares are trustlines whose asset is the pool id. Lookups: pool_id, a pair (asset_a + asset_b, in the protocol's order).$$;

-- Ingest progress: the newest ledger the API can answer for.
CREATE VIEW api.status AS
SELECT s.ledger_sequence AS latest_ledger,
       s.ledger_hash     AS latest_ledger_hash,
       l.closed_at       AS latest_ledger_closed_at,
       s.updated_at      AS ingested_at,
       (SELECT min(m.sequence) FROM explorer.ledgers m) AS oldest_ledger,
       2                 AS api_version
  FROM public.ingest_state s
  LEFT JOIN explorer.ledgers l ON l.sequence = s.ledger_sequence
 WHERE s.pipeline = 'explorer';
COMMENT ON VIEW api.status IS $$Ingest status
One row: the newest ingested ledger, when it closed and was ingested, and the oldest ledger the API has (the state's checkpoint).$$;

-- Computed relationships ---------------------------------------------------
-- PostgREST embeds these, e.g. /accounts?account_id=eq.G…&select=*,trustlines(*),offers(*).
-- They must stay LANGUAGE sql, STABLE and SETOF so PostgreSQL inlines them.
-- Each is named after the view it returns: PostgREST 16.4 applies the domain
-- representations of an embedded resource only when the two names match.

CREATE FUNCTION api.trustlines(api.accounts) RETURNS SETOF api.trustlines
    LANGUAGE sql STABLE ROWS 10
    AS $$ SELECT * FROM api.trustlines tl WHERE tl.account_id = $1.account_id $$;

CREATE FUNCTION api.sac_balances(api.accounts) RETURNS SETOF api.sac_balances
    LANGUAGE sql STABLE ROWS 10
    AS $$ SELECT * FROM api.sac_balances b WHERE b.balance_holder = $1.account_id $$;

CREATE FUNCTION api.data_entries(api.accounts) RETURNS SETOF api.data_entries
    LANGUAGE sql STABLE ROWS 5
    AS $$ SELECT * FROM api.data_entries d WHERE d.account_id = $1.account_id $$;

CREATE FUNCTION api.offers(api.accounts) RETURNS SETOF api.offers
    LANGUAGE sql STABLE ROWS 10
    AS $$ SELECT * FROM api.offers o WHERE o.seller_id = $1.account_id $$;

CREATE FUNCTION api.claimable_balances(api.accounts) RETURNS SETOF api.claimable_balances
    LANGUAGE sql STABLE ROWS 5
    AS $$ SELECT * FROM api.claimable_balances c
          WHERE c.claimants @> jsonb_build_array(jsonb_build_object('destination', $1.account_id)) $$;

CREATE FUNCTION api.contract_code(api.contracts) RETURNS SETOF api.contract_code
    LANGUAGE sql STABLE ROWS 1
    AS $$ SELECT * FROM api.contract_code c WHERE c.code_hash = $1.wasm_hash $$;

-- The order book of a pair, by price level: asks sell base for counter, bids
-- buy base with counter. Both sides' price is counter per unit of base, and
-- their amount in base units (stroops); the best levels come first. It reads
-- the view: a function runs with the caller's rights, and web_anon may not
-- read schema explorer. Price and amount are text: PostgREST applies the
-- domains' JSON representation to view columns only.
--   GET /rpc/order_book?base=native&counter=USDC:G…&depth=20
CREATE FUNCTION api.order_book(base text, counter text, depth integer DEFAULT 20)
    RETURNS TABLE (side text, price text, amount text, offers bigint)
    LANGUAGE sql STABLE ROWS 40
    AS $$
    (SELECT 'ask'::text, o.price::text, sum(o.amount)::text, count(*)
       FROM api.offers o
      WHERE o.selling = $1 AND o.buying = $2
      GROUP BY o.price ORDER BY o.price LIMIT least(greatest($3, 1), 200))
    UNION ALL
    (SELECT 'bid'::text, trim_scale(round(1 / o.price, 7))::text, round(sum(o.amount * o.price))::text, count(*)
       FROM api.offers o
      WHERE o.selling = $2 AND o.buying = $1
      GROUP BY o.price ORDER BY o.price LIMIT least(greatest($3, 1), 200))
    $$;
COMMENT ON FUNCTION api.order_book(text, text, integer) IS $$The order book of a pair (base, counter), by price level
Asks sell base, bids buy it; price is counter per unit of base; amount is in stroops of base. depth levels per side (default 20, at most 200).$$;

-- Grants -------------------------------------------------------------------

DO $$
DECLARE
    anon name := coalesce(nullif(current_setting('stellar.api_anon_role', true), ''), 'web_anon');
    g    record;
BEGIN
    -- Default privileges (roles.sql) also hand explorer_owner's
    -- new schemas and views to ingest_writer, and functions are executable by
    -- PUBLIC. Remove every grant but the owner's: the API is for anon only.
    FOR g IN
        SELECT DISTINCT a.grantee
          FROM pg_class c, aclexplode(c.relacl) a
         WHERE c.relnamespace IN ('api'::regnamespace, 'api_types'::regnamespace) AND a.grantee <> c.relowner
        UNION
        SELECT a.grantee
          FROM pg_namespace n, aclexplode(n.nspacl) a
         WHERE n.nspname IN ('api', 'api_types') AND a.grantee <> n.nspowner
        UNION
        SELECT a.grantee
          FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
         WHERE p.pronamespace IN ('api'::regnamespace, 'api_types'::regnamespace) AND a.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA api, api_types FROM %s',
                       CASE g.grantee WHEN 0 THEN 'PUBLIC' ELSE g.grantee::regrole::text END);
        EXECUTE format('REVOKE ALL ON SCHEMA api, api_types FROM %s',
                       CASE g.grantee WHEN 0 THEN 'PUBLIC' ELSE g.grantee::regrole::text END);
        EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA api, api_types FROM %s',
                       CASE g.grantee WHEN 0 THEN 'PUBLIC' ELSE g.grantee::regrole::text END);
    END LOOP;

    EXECUTE format('GRANT USAGE ON SCHEMA api, api_types TO %I', anon);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA api TO %I', anon);
    EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO %I', anon);
END
$$;

-- PostgREST loads the domain representations as the authenticator, which
-- does not inherit web_anon's privileges, and skips casts whose function it
-- may not execute. The cast functions only format values.
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api_types TO PUBLIC;

NOTIFY pgrst, 'reload schema';

COMMIT;
