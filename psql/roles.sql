-- roles.sql: the database explorer, its roles and their default privileges.
--
-- lab psql (step database) runs it as the postgres superuser on every run
-- whose file differs from the one last applied (README §6.3).
-- It is idempotent: it creates what is missing and resets
-- attributes and privileges to what is written here. By hand:
--   runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -f - < roles.sql
-- No passwords here: lab psql sets explorer_owner's and ingest_writer's as
-- SCRAM verifiers it computes itself, so the plaintext never reaches the
-- server. authenticator, postgres_exporter and state_reader log in over the
-- Unix socket with peer authentication (pg_hba.conf, pg_ident.conf) and have
-- none.

SET client_min_messages = warning;   -- not "already a member", "already exists"

-- Roles ----------------------------------------------------------------------

DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['explorer_owner', 'ingest_writer', 'authenticator', 'web_anon', 'postgres_exporter', 'state_reader'] LOOP
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('CREATE ROLE %I', r);
        END IF;
    END LOOP;
END
$$;

-- Owns the database, the schemas and every object: `worker migrate` (from the
-- galexie host) and api.sql (here, through SET ROLE) run as this role.
ALTER ROLE explorer_owner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
-- worker ingest, bootstrap and reindex.
ALTER ROLE ingest_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
-- PostgREST's login role: no privileges of its own, it switches to web_anon
-- per request. NOINHERIT keeps web_anon's grants out of its own session.
ALTER ROLE authenticator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD NULL;
-- The anonymous API role: SELECT on the api views only (api.sql).
ALTER ROLE web_anon NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
-- PostgREST applies the switched-to role's settings per request (db-config).
ALTER ROLE web_anon SET statement_timeout = '2s';
-- Monitoring (pg_monitor), peer authentication only.
ALTER ROLE postgres_exporter LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD NULL;
-- worker state-copy (the OS user stellar, clickhouse/README.md §10): reads the state it
-- copies to ClickHouse (grants.sql), peer authentication only.
ALTER ROLE state_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD NULL;
ALTER ROLE state_reader SET default_transaction_read_only = on;

GRANT web_anon TO authenticator;
GRANT pg_monitor TO postgres_exporter;

-- Database -------------------------------------------------------------------

-- CREATE DATABASE has no IF NOT EXISTS and cannot run in a DO block.
SELECT 'CREATE DATABASE explorer OWNER explorer_owner'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'explorer')
\gexec

ALTER DATABASE explorer OWNER TO explorer_owner;
REVOKE ALL ON DATABASE explorer FROM PUBLIC;
-- TEMPORARY: ingest batches its COPYs through temp tables.
GRANT CONNECT, TEMPORARY ON DATABASE explorer TO ingest_writer;
GRANT CONNECT ON DATABASE explorer TO authenticator, postgres_exporter, state_reader;

\connect explorer
SET client_min_messages = warning;

-- Needs shared_preload_libraries (conf.d/20-tuning.conf) to collect anything.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Default privileges: whatever explorer_owner creates from now on (schema
-- explorer, its tables and public.ingest_state) is usable by ingest_writer
-- without further grants, so `worker bootstrap` (which truncates the state
-- tables) can run right after `worker migrate`. TRUNCATE removes no more
-- than DELETE could. They are per database and survive DROP SCHEMA (the
-- reset in README §8.8).
-- api.sql revokes them again on its own schemas.
ALTER DEFAULT PRIVILEGES FOR ROLE explorer_owner GRANT USAGE ON SCHEMAS TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE explorer_owner GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE explorer_owner GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE explorer_owner GRANT EXECUTE ON FUNCTIONS TO ingest_writer;
