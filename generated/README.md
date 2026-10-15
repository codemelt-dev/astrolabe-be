# Generated: TypeScript types of the explorer API

[`api.ts`](api.ts) is the `Database` type of the public API: every view and embedded function of schema `api` ([`psql/api.sql`](../psql/api.sql)), as [PostgREST](https://postgrest.org) serves it. It is generated: never edit it by hand. Regenerate it whenever `api.sql` (or a worker migration the views read) changes, and commit it with that change.

## Using it

With [`@supabase/postgrest-js`](https://www.npmjs.com/package/@supabase/postgrest-js), the standalone PostgREST client: no Supabase involved. Copy `api.ts` into the frontend, or import it from this repository.

```ts
import { PostgrestClient } from '@supabase/postgrest-js'
import type { Database } from './api'

const api = new PostgrestClient<Database>('https://api.example.com', { schema: 'api' })

// Columns, filters and order are checked; the row type follows the select string.
const { data: ledgers } = await api
  .from('ledgers')
  .select('sequence, hash, closed_at, op_count')
  .order('sequence', { ascending: false })
  .limit(20)

// Embedded functions (computed relationships) are typed too.
const { data: account } = await api
  .from('accounts')
  .select('account_id, balance, trustlines(asset, balance), offers(offer_id, selling, buying, amount, price)')
  .eq('account_id', accountId)
  .maybeSingle()

// Functions too: the order book of a pair.
const { data: book } = await api.rpc('order_book', { base: 'native', counter: usdc, depth: 20 })
```

The history (transactions, operations, events) isn't in this API: it comes from ClickHouse's public read-only SQL ([`clickhouse/README.md`](../clickhouse/README.md) §8).

What the types say, and why:

- **Strings for 64-bit integers, amounts, hashes and XDR.** The API sends `api_types.int64`, `amount`, `hex` and `base64` columns as JSON strings (no precision loss in JavaScript), and the types say `string`. Plain `integer` and `bigint` columns that always fit (ledger sequences, indexes, counts) are numbers.
- **Nullability.** Postgres doesn't record `NOT NULL` for view columns. The generator makes a column non-null when it is a `NOT NULL` column of the same name in a table the view reads, unless that table is the outer side of a `LEFT JOIN`. So `contract_data.live_until_ledger` (from the joined TTL entry) is `number | null`, `contract_data.contract_id` is `string`. The rule only ever errs towards `| null`.
- **Read-only.** The views have no `Insert`/`Update` types. Caddy rejects every method but `GET`, `HEAD` and `OPTIONS` anyway.
- **Paging.** Use keyset filters on the primary-key columns (`.lt('sequence', last)`), not `.range()` with large offsets (psql/README.md, the API section).
- **`PostgrestVersion: "14"`** in the types turns on postgrest-js's features for PostgREST 13 and later. The server runs v16, a superset.

## Regenerating

It needs Node 20+ and a database with the `api` schema applied. A local one is the way: the schema is entirely the repository's (`roles.sql`, the worker's migrations, `grants.sql`, `api.sql`), so the types never depend on a server's data, and the psql host allows no TCP logins from outside its galexie peer anyway.

```sh
cd generated
npm ci
DATABASE_URL=postgres://localhost:5432/explorer npm run gen     # writes api.ts
DATABASE_URL=postgres://localhost:5432/explorer npm run check   # diff only: fails if api.ts is stale
```

Or from the repository root: `make types DATABASE_URL=…` and `make types-check DATABASE_URL=…`.

A local database with the API, as a Postgres 18 superuser (the SQL test in psql/README.md, §9, does the same):

```sh
psql -X -v ON_ERROR_STOP=1 -d postgres -f psql/roles.sql
psql -X -d postgres -c "ALTER ROLE explorer_owner PASSWORD 'x'"
worker migrate --config <worker.toml with [postgres] migrate_dsn = "postgres://explorer_owner:x@localhost:5432/explorer?sslmode=disable">
(echo 'SET ROLE explorer_owner;'; cat psql/grants.sql psql/api.sql) | psql -X -v ON_ERROR_STOP=1 -d explorer -f -
```

Drop it afterwards with `DROP DATABASE explorer` and `DROP ROLE explorer_owner, ingest_writer, authenticator, web_anon, postgres_exporter`.

## How it works

[`gen.mjs`](gen.mjs) runs [`@supabase/postgrest-typegen`](https://www.npmjs.com/package/@supabase/postgrest-typegen), the type generator behind `supabase gen types` as a library, straight against Postgres (no Docker, no Supabase CLI). It adjusts the introspected metadata before generating: the `api_types` domains become strings, the nullability rule above, no write types, and the views' row types named without their schema (which postgrest-js needs to type embedded functions). Versions are pinned in [`package.json`](package.json) and `package-lock.json`; postgrest-typegen is still marked alpha upstream, so bump it deliberately and compare the output.

| File | What |
|---|---|
| `api.ts` | the generated types (commit it) |
| `gen.mjs` | the generator: introspect, adjust, generate |
| `package.json`, `package-lock.json` | pinned generator dependencies; `npm run gen`, `npm run check` |
