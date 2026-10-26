# Generated: TypeScript types for the REST API

[`api.ts`](api.ts) contains the `Database` type for the public REST API: every view and function in the `api` schema ([`psql/api.sql`](../psql/api.sql)), as PostgREST serves it. The file is generated and should not be edited by hand. It is regenerated whenever `api.sql`, or a worker migration that the views depend on, changes, and is committed together with that change.

## Usage

The types work with [`@supabase/postgrest-js`](https://www.npmjs.com/package/@supabase/postgrest-js), the standalone PostgREST client (no Supabase service is involved).

```ts
import { PostgrestClient } from '@supabase/postgrest-js'
import type { Database } from './api'

const api = new PostgrestClient<Database>('https://api.example.com', { schema: 'api' })

// Columns, filters and ordering are type-checked; the row type follows the select string.
const { data: ledgers } = await api
  .from('ledgers')
  .select('sequence, hash, closed_at, op_count')
  .order('sequence', { ascending: false })
  .limit(20)

// Embedded relationships are typed as well.
const { data: account } = await api
  .from('accounts')
  .select('account_id, balance, trustlines(asset, balance), offers(offer_id, selling, buying, amount, price)')
  .eq('account_id', accountId)
  .maybeSingle()

// So are functions, e.g. the order book of an asset pair.
const { data: book } = await api.rpc('order_book', { base: 'native', counter: usdc, depth: 20 })
```

Transaction, operation and event history is not part of this API. It is served by ClickHouse as read-only SQL ([`clickhouse/README.md`](../clickhouse/README.md#public-read-only-sql)).

## What the types encode

- **Strings for large numbers, hashes and XDR.** Columns of the `api_types` domains (`int64`, `amount`, `hex`, `base64`) are sent as JSON strings so that JavaScript loses no precision, and are typed as `string`. Integer columns that always fit in a JavaScript number (ledger sequences, counts) are typed as `number`.
- **Nullability.** PostgreSQL does not record `NOT NULL` for view columns. The generator marks a view column as non-null when it maps to a `NOT NULL` column of the same name in a table the view reads, unless that table is on the outer side of a `LEFT JOIN`. The rule only ever errs towards `| null`.
- **Read-only.** No `Insert`/`Update` types are generated; the public endpoint only accepts `GET`, `HEAD` and `OPTIONS`.
- **Paging.** Keyset filters on key columns (`.lt('sequence', last)`) are the supported way to page; large offsets run into the API's statement timeout.

## Regenerating

Requirements: Node 20+ and a local PostgreSQL 18 database with the `api` schema applied. The schema is defined entirely by the repository, so the types never depend on a server's data.

Create the local database (as a PostgreSQL superuser):

```sh
psql -X -v ON_ERROR_STOP=1 -d postgres -f psql/roles.sql
psql -X -d postgres -c "ALTER ROLE explorer_owner PASSWORD 'x'"
worker migrate --config <worker.toml with [postgres] migrate_dsn = "postgres://explorer_owner:x@localhost:5432/explorer?sslmode=disable">
(echo 'SET ROLE explorer_owner;'; cat psql/grants.sql psql/api.sql) | psql -X -v ON_ERROR_STOP=1 -d explorer -f -
```

Generate or check the types:

```sh
make types       DATABASE_URL=postgres://localhost:5432/explorer   # writes generated/api.ts
make types-check DATABASE_URL=postgres://localhost:5432/explorer   # fails if api.ts is out of date
```

Clean up with `DROP DATABASE explorer` and `DROP ROLE explorer_owner, ingest_writer, authenticator, web_anon, postgres_exporter, state_reader`.

## How it works

[`gen.mjs`](gen.mjs) runs [`@supabase/postgrest-typegen`](https://www.npmjs.com/package/@supabase/postgrest-typegen) as a library directly against PostgreSQL (no Docker or Supabase CLI). Before generating, it adjusts the introspected metadata: the `api_types` domains become strings, the nullability rule above is applied, write types are dropped, and view row types are named without their schema (which postgrest-js needs to type embedded relationships). Versions are pinned in [`package.json`](package.json) and `package-lock.json`; the generator is still marked alpha upstream, so upgrades should be checked by comparing the output.

| File | Purpose |
|---|---|
| `api.ts` | the generated types (committed) |
| `gen.mjs` | the generator: introspect, adjust, generate |
| `package.json`, `package-lock.json` | pinned dependencies; `npm run gen`, `npm run check` |
