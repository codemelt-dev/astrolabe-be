# clickhouse host

Runbook for the explorer's history server: **ClickHouse 26.8** (LTS) with all decoded history since go-live, kept for good, written by the worker's **clickhouse pipeline** (`worker clickhouse-ingest`), which reads every ledger from the AWS public lake. It also holds a daily copy of the current state, the asset list, supplies and top holders computed from it, their daily snapshots, and daily statistics. The psql host's Caddy serves it to the internet as **public read-only SQL**: anonymously, and to the frontend's backend with its own login (§8). Its Grafana has a ClickHouse dashboard.

PostgreSQL holds the current state and serves it through the API ([`psql/README.md`](../psql/README.md)); ClickHouse holds the history, the rankings and the statistics, each where it measured best (§1). A transaction's raw XDR comes from its ledger in the AWS public lake, found through `tx_by_hash`.

**`lab clickhouse`**, part of the repository's `lab` CLI ([`cli/`](../cli)), does every step on the server: it checks each step, applies what is not in the documented state, and checks again. A second run changes nothing, and `lab clickhouse check` reports the state and the host's health without changing anything. This README says what it does and why, and covers what it can't do: ordering the server, deploying a new worker, and day-2 operations.

## Contents

1. [What runs here](#1-what-runs-here)
2. [Disk and growth](#2-disk-and-growth)
3. [lab clickhouse](#3-lab-clickhouse)
4. [Bring-up with the psql host](#4-bring-up-with-the-psql-host)
5. [ClickHouse](#5-clickhouse)
6. [Schema](#6-schema)
7. [The clickhouse pipeline](#7-the-clickhouse-pipeline)
8. [Public read-only SQL](#8-public-read-only-sql)
9. [Operations](#9-operations)
10. [The state copy, assets and top holders](#10-the-state-copy-assets-and-top-holders)
11. [Limitations](#11-limitations)
12. [Development](#12-development)
13. [Files](#13-files)

---

## 1. What runs here

```text
  galexie host 10.0.0.2                     archive bucket (HEL1)          AWS public lake
  Galexie → lake → archiver ──────────────▶ every ledger, forever          full history (SEP-54)
             └─▶ ingest ── TLS 5432 ──▶ psql host 10.0.1.2   │                    │
                                                             │ init: go-live      │ every ledger
  clickhouse host 10.0.2.2 ──────────────────────────────────┴────────────────────┘
  │ worker clickhouse-ingest ──▶ ClickHouse (127.0.0.1:9000)
  │ private IP: 8443 / 9440 (TLS), 9363, 9100, 9103 (metrics), only from the psql host
  └─ psql host: Prometheus scrapes it; Grafana queries it as stellar_reader;
                worker state-copy writes the state copy daily as stellar_jobs (9440);
                Caddy serves it to the internet as stellar_public, or as stellar_api for codemelt
```

Why it is built this way:

- **ClickHouse has its own server.** Merges and large queries want every core and the page cache. Next to PostgreSQL they would compete for memory with the database the API depends on, on the one host that faces the internet.
- **The pipeline runs here and reads the AWS public lake, not the lake on the galexie host.**
  - The galexie host stays as it is: no new process, memory, port or firewall rule. It is the tightest host.
  - Not the archive bucket either: Hetzner's object storage can stall on a single object for minutes (a 504 after 60 s, again and again), which holds up an in-order catch-up. The AWS lake trails the network by about a ledger and holds every ledger since genesis; the archive bucket only marks go-live for `init` (§7.4).
  - The read needs no object checksum: the full decode checks the zstd frame checksums, the header's hash and the hash chain.
  - A stopped or slow ClickHouse can't hold back the explorer pipeline or the lake's pruning. The pruner only waits for the pipelines in its own process.
  - Rebuilds, and a later backfill, run where the data lands.
- **One decoder.** The pipeline runs the worker's `processLedger` for history (`explorer.HistoryProcessor`); the explorer pipeline on the galexie host runs it for state.
- **History here, current state in PostgreSQL.** Current state is per-key upserts and key lookups, which ClickHouse handles poorly (10–100× slower, measured); rankings over the whole state are the reverse. So the rankings run here, on a copy of the state that `worker state-copy` brings over from the psql host once a day (§10).
- **The close time on every row,** so time series need no join; all events, fee and refund events included, with every topic.
- **Nothing expires.** History grows about 2 GiB a day (§2).
- **No backups** (decided). Everything but the snapshots comes back from the AWS lake (§9.6).

---

## 2. Disk and growth

The data lives in `/var/lib/clickhouse` on the root filesystem; the host holds nothing else.

**Measured on pubnet**, over 8 days of history: about **1.7 GiB a day** of history, plus the day's snapshots.

| Table | per day |
|---|---|
| `operations` | 551 MiB |
| `transactions` | 404 MiB |
| `events` | 210 MiB |
| `token_transfers`, `transfers_by_account` | 183 MiB each |
| `account_activity` | 95 MiB |
| `tx_by_hash` | 79 MiB |
| `contract_calls` | 55 MiB |
| `ledgers`, `contract_creations` | < 2 MiB |
| `holder_snapshots`, `asset_snapshots` | ~0.2 GiB (one set a day; the first is larger, ~380 MB) |

The state copy, `assets` and `top_holders` don't grow: each run replaces them, ~1.9 GiB at any time.

- **In all ~2 GiB a day, ~0.7 TiB a year, and nothing expires.** At this rate a 320 GB disk reaches the alerts' 70 % after about three months and fills after about four and a half: grow it in time (a larger server or volume). The alerts warn at 70 % and 85 % and when the disk would fill within 30 days (§9.4).
- **Treat it as ±30 %:** traffic varies by hour and day. Measure: `SELECT table, formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE active AND database = 'stellar' GROUP BY table ORDER BY sum(bytes_on_disk) DESC`.
- **The system logs** add a few GB at most: they keep 14 days (§9.5).

---

## 3. lab clickhouse

### 3.1 Build and run

As for the other hosts ([README.md](../README.md#deploy)): `make dist VERSION=…` on the workstation, copy `bin/` to the host, run `lab` as root.

```sh
scp -r bin root@<clickhouse public IP>:/root/
ssh root@<clickhouse public IP> /root/bin/lab clickhouse          # asks for its settings and the archive bucket's key
ssh root@<clickhouse public IP> /root/bin/lab clickhouse check    # read-only: state and health
```

### 3.2 Steps

| # | Step | Does | Health |
|---|---|---|---|
| 1 | `system` | packages, UTC, sshd hardening, journald cap, needrestart (shared with the psql host) | |
| 2 | `network` | optional: the VLAN interface with the private IP ([`vswitch.yaml`](files/vswitch.yaml)) | the psql host answers |
| 3 | `firewall` | [`nftables.conf`](files/nftables.conf): the whole ruleset, default drop | ruleset loaded |
| 4 | `clickhouse` | the apt repository ([`clickhouse.sources`](files/clickhouse.sources); its key checked by sha256), the pinned and held packages, node_exporter, [`stellar-config.xml`](files/stellar-config.xml), [`stellar-users.xml`](files/stellar-users.xml), the users' password hashes, TLS, root's `clickhouse-client` login, `CREATE DATABASE stellar` | `/ping` over HTTPS on the private IP, trusting lab's CA; certificate days left |
| 5 | `worker` | user `stellar`; the worker shipped next to `lab` (§9.2) | |
| 6 | `config` | `/etc/stellar`: [`worker.toml`](files/worker.toml), `archive.env` (the archive key, for `init`), the writer's and owner's password files; the clickhouse bundle for the psql host (§4) | |
| 7 | `migrate` | `worker clickhouse-migrate` while migrations are pending | |
| 8 | `init` | `worker clickhouse-init` once: the cursor before the archive's oldest ledger (§7.4) | |
| 9 | `services` | [`stellar-clickhouse-ingest.service`](files/stellar-clickhouse-ingest.service) | `/readyz`, cursor and lag behind the AWS lake's tip |

### 3.3 Settings and secrets

Settings are asked once and kept in `/etc/stellar/lab-clickhouse.toml`:

| Key | Example | Notes |
|---|---|---|
| `private_ip` | `10.0.2.2` | |
| `psql_ip` | `10.0.1.2` | the firewall and the users' networks |
| `private_if`, `vlan_id`, `vlan_link`, `vlan_gateway`, `cloud_network` | `enp5s0.4000`, `none` | as on the psql host; `vlan_id = "none"` on a Cloud server |
| `admin_ips` | `203.0.113.10` | SSH |
| `archive_bucket`, `archive_endpoint`, `archive_region` | as on the galexie host | |
| `archive_access_key`, `archive_secret_key` | secrets | a key that can read the archive bucket, such as the galexie host's |

Generated once into `/etc/stellar/secrets/` (root 0600), and never rotated silently:

| Secret | What |
|---|---|
| `clickhouse-<user>.password` | one per user (§5.3): `default`, `stellar_owner`, `stellar_writer`, `stellar_reader`, `stellar_public`, `stellar_api` (Caddy logs in with it for the frontend's backend), `stellar_jobs` |
| `clickhouse-ca.key` | lab's CA for this host's TLS; its certificate is `/etc/stellar/clickhouse-ca.crt` |

Copy them into the password manager. A rebuilt server without them gets new ones, and the psql host then needs the new bundle (§4).

### 3.4 Restarts

A running service whose files changed is **not** restarted unless you pass `--allow-restart`: the step shows a pending restart and the run exits 1.

- **ClickHouse** restarts for a new package, `config.d` or the certificate. `users.d` is re-read without one. A restart pauses the pipeline for a few seconds; it retries and catches up by itself.
- **The pipeline** restarts for its unit, `worker.toml` or `archive.env`. A new worker binary is a deploy (§9.2).

---

## 4. Bring-up with the psql host

The psql host reaches ClickHouse over the private network: Prometheus for metrics, Grafana for its dashboard, Caddy for the public SQL, and `worker state-copy` for the state copy. What it needs comes in the **clickhouse bundle**, which `lab clickhouse` writes to `/root/clickhouse-bundle/`: lab's CA and the passwords of `stellar_reader`, `stellar_public`, `stellar_jobs`, and `stellar_api` (for the psql host's health check).

```sh
ssh root@<clickhouse public IP> /root/bin/lab clickhouse
scp -3 -r root@<clickhouse public IP>:/root/clickhouse-bundle root@<psql public IP>:/root/
# on the psql host: set clickhouse_hostname (a DNS name for the public SQL) and clickhouse_ip
# in /etc/stellar/lab-psql.toml, then
/root/bin/lab psql
/root/bin/lab grafana     # the ClickHouse data source and dashboard (dashboard/README.md)
```

`lab psql`'s `clickhouse` step ([psql/README.md §3.2](../psql/README.md#32-steps)) installs the bundle, the Caddy site, and with monitoring on, the scrape jobs ([`psql/files/clickhouse-scrape.yml`](../psql/files/clickhouse-scrape.yml)) and this host's alerts ([`files/alerts.yml`](files/alerts.yml)). Its `jobs` step installs the worker there with the daily state-copy timer (§10), and its `grafana` step adds the ClickHouse data source and dashboard ([dashboard/README.md](../dashboard/README.md#clickhouse)). `clickhouse_hostname = "none"` takes them all away again.

The galexie host doesn't change. Its alert rules and pipeline dashboard are scoped to `job="ingest"`, because the clickhouse pipeline exports the same metric names.

---

## 5. ClickHouse

### 5.1 Version and install

- **ClickHouse 26.8.19.9**, the LTS of the time, from the official apt repository's LTS channel. The repository key is checked against a pinned sha256 (fingerprint `3A9EA1193A97B548BE1457D48919F6BD2B48D754`).
- `clickhouse-server`, `clickhouse-client` and `clickhouse-common-static` are pinned to that version and held with `apt-mark hold`, so an upgrade is always deliberate (§9.3).
- A single node: no Keeper, no replication.

### 5.2 Server configuration

[`files/stellar-config.xml`](files/stellar-config.xml) → `/etc/clickhouse-server/config.d/10-stellar.xml`, layered over the package's `config.xml`:

| Setting | Why |
|---|---|
| `listen_host` 127.0.0.1 and the private IP | never the public interface |
| HTTP 8123 and native 9000 | plain; nftables keeps them to loopback (the worker, `clickhouse-client`) |
| HTTPS 8443 and native TLS 9440 | for the psql host, with lab's certificate (§5.4) |
| interserver, MySQL and PostgreSQL ports removed | a single server, and no other wire protocols |
| Prometheus endpoint on 9363 | metrics, profile events, asynchronous metrics |
| `max_server_memory_usage_to_ram_ratio` 0.75 | room for the pipeline and the page cache |
| ZSTD(3) for every part | history is written once and read in bulk; the default is LZ4 |
| a 14-day TTL on the system logs | by default most grow without limit (§9.5) |
| UTC; server logs capped at 10 × 100 MB | like the other hosts |

### 5.3 Users

[`files/stellar-users.xml`](files/stellar-users.xml) → `/etc/clickhouse-server/users.d/10-stellar.xml`. Users are defined in files, not with SQL (`access_management` stays off), so `lab` manages them like any other configuration. `lab` writes the passwords' SHA-256 into `users.d/90-passwords.xml`, which also removes the package's empty password for `default`.

| User | From | Can | Profile and quota | Used by |
|---|---|---|---|---|
| `default` | loopback | everything | | `lab`, the operator's `clickhouse-client` (root's login is in `/root/.clickhouse-client/config.xml`) |
| `stellar_owner` | loopback | DDL on `stellar.*` | | `worker clickhouse-migrate` |
| `stellar_writer` | loopback | `SELECT`, `INSERT`, `ALTER DELETE` on `stellar.*` | `writer`: 8 GB | `worker clickhouse-ingest`, `clickhouse-init`, `clickhouse-check` |
| `stellar_reader` | loopback, the psql host | `SELECT` on `stellar.*`; the schema browser's `system.databases`, `tables`, `columns` | `reader`: read only, 30 s (at most 60), 4 GB, 4 threads, 1,000,000 rows; 20,000 queries an hour | Grafana |
| `stellar_public` | loopback, the psql host | `SELECT` on `stellar.*` | `public`: read only, 10 s, 2 GB, 2 threads, 100,000 rows; 30,000 queries an hour | the public SQL endpoint, anonymous (§8) |
| `stellar_api` | loopback, the psql host | `SELECT` on `stellar.*` | `api`: read only, 10 s, 1 GB (4 GB in all), 1 thread (at most 4) and the uncompressed cache, 1,000,000 rows, 16 concurrent queries; 720,000 queries and 14,400 s an hour | the frontend's backend, as `codemelt` through the public endpoint (§8) |
| `stellar_jobs` | the psql host | create, fill, swap and drop the state copy, `assets` and `top_holders` (and their `_new` tables); insert into and delete from the snapshots; insert into `state_copy_runs`; read `transfers_by_account` and `contract_creations` | `jobs`: 6 GB, 30 min | `worker state-copy` (§10) |

- **`readonly = 2`** lets a client adjust settings within the profile's constraints, never write or change `readonly` itself.
- **The system tables need a grant** (`select_from_system_db_requires_grant`): neither Grafana nor the public can read `system.query_log`, which holds everyone's queries.
- **The worker reads its passwords** from `/etc/stellar/clickhouse-writer.password` (root:stellar 0640) and `clickhouse-owner.password` (root 0600).

### 5.4 TLS

`lab clickhouse` creates a CA and a server certificate for the private IP, the way `lab psql` does for PostgreSQL. The psql host only ever uses the TLS ports, 8443 (Caddy) and 9440 (Grafana, `worker state-copy`), trusting that CA. Clients aren't asked for certificates.

### 5.5 Firewall

[`files/nftables.conf`](files/nftables.conf): the whole ruleset, default drop.

| Port | From | What |
|---|---|---|
| 22 | admin IPs | SSH |
| 8443, 9440 | the psql host | ClickHouse over TLS (Caddy, Grafana, the state copy) |
| 9363 | the psql host | ClickHouse's metrics |
| 9100 | the psql host | node_exporter |
| 9103 | the psql host | the pipeline's `/metrics` |
| 8123, 9000 | loopback | the worker, `clickhouse-client` |

---

## 6. Schema

**The worker's migrations own the database `stellar`**, as they own the `explorer` schema in PostgreSQL: [`worker/clickhouse/*.sql`](../worker/clickhouse/), next to the code that writes the tables. Same format (`-- +migration <v> <name>` sections, checksummed, forward-only), applied by `worker clickhouse-migrate` as `stellar_owner` and recorded in `stellar.schema_migrations`. **Never edit an applied migration**: add a new one. This folder owns the server, the users and the empty database.

The tables (13 migrations, no TTL):

| Table | `ORDER BY` | Written by | Notes |
|---|---|---|---|
| `ledgers` | `sequence` | the pipeline | |
| `transactions` | `(ledger_sequence, tx_index)` | the pipeline | bloom filters on `hash` and `source_account` |
| `operations` | `(ledger_sequence, tx_index, op_index)` | the pipeline | `type_name` (snake_case of the XDR name); `details` JSON; `result_code` (the operation's own result: `payment_underfunded`, `op_bad_auth` …); `return_value` (a Soroban call's, JSON) |
| `events` | `(contract_id, ledger_sequence, tx_index, op_index, event_index)` | the pipeline | `stage` is fee, refund or operation; `topics Array(String)`, every topic |
| `token_transfers` | `(contract_id, ledger_sequence, tx_index, op_index, position)` | the pipeline | kind, asset, from, to, `amount Int128` |
| `transfers_by_account` | `(account, ledger_sequence, tx_index, op_index, position)` | the view `transfers_by_account_mv` | one row per side, negative when the account sends |
| `tx_by_hash` | `prefix` (8 bytes of the hash) | the pipeline | every transaction, and a fee bump's inner transaction too: hash → ledger and index; check the whole hash against `transactions`, since 8 bytes can collide |
| `account_activity` | `(account, ledger_sequence, tx_index)` | the pipeline | every transaction an account or contract took part in; `roles`: 1 source, 2 fee payer, 4 operation source, 8 named in an operation, 16 a side of a token transfer, 32 owns a changed ledger entry |
| `contract_calls` | `(contract_id, ledger_sequence, tx_index, op_index)` | the pipeline | each `invoke_contract`: function, caller, outcome, return value |
| `contract_creations` | `contract_id` | the pipeline | who created each contract and when; deployments by other contracts included (`via_contract`) |
| `daily_stats` | `day` | the refreshable view `daily_stats_mv`, every 5 minutes | read with `FINAL` |
| `state_accounts`, `state_trustlines`, `state_sac_balances`, `state_asset_contracts`, `state_claimable_balances`, `state_liquidity_pools` | for the rankings | `worker state-copy`, daily (§10) | PostgreSQL's state as of `state_copy_runs.ledger`, replaced whole |
| `assets`, `top_holders` | `asset`; `(asset, rank)` | `worker state-copy` | the asset list with supplies; the top 500 of each asset, 1,000 of XLM |
| `asset_snapshots`, `holder_snapshots` | `(asset, date)`; `(asset, date, rank)` | `worker state-copy --snapshot` | `assets` and `top_holders` by date, kept for good |
| `state_copy_runs` | `started_at` | `worker state-copy` | each run: the ledger, the rows, the snapshot's date |
| `ingest_state` | `pipeline` | the pipeline | the cursor (§7.2) |

Rules for every history table:

- **Each row carries `ledger_sequence` and `closed_at`**, so time series need no join with `ledgers`.
- **Partitions are monthly**, by `toYYYYMM(closed_at)` (`tx_by_hash`: by a million ledgers; `contract_creations`: none).
- **Nothing expires.**
- **`ORDER BY` follows the table's main access path.** Where `ledger_sequence` isn't first, a `minmax` index on it keeps ledger ranges cheap, and with them the recovery delete (§7.3).
- **Types:** hashes `FixedString(32)` (raw bytes); strkeys `String`; JSON `String`, read with `JSONExtract*` (an ScVal is an object keyed by its type: `{"symbol":"transfer"}`, `{"i128":"63475"}`, `{"address":"G…"}`); amounts `Int128` in base units, fees `Int64`.
- **No `Nullable`:** empty strings and zeros stand in for "absent".

Things to know when querying:

- **Token transfer amounts can be negative:** a fee refund is a negative `fee`. `transfers_by_account` turns it into a credit to the payer.
- **A column alias is visible in the whole query**, ARRAY JOIN and WHERE included. An alias with a column's name changes what the rest of the query reads (`countIf(successful) AS successful` becomes a nested aggregate). Give aliases their own names, or qualify the column (`token_transfers.amount`).
- **Aggregates are refreshable views** that recompute the last days into a `ReplacingMergeTree` keyed by day, which makes them idempotent: `daily_stats` recomputes the two newest days the pipeline has reached every 5 minutes, so a re-ingest computes each day as it passes. Not insert-triggered aggregating views: rows deleted by the recovery step (§7.3) would stay counted in them.

---

## 7. The clickhouse pipeline

### 7.1 How it runs

`worker clickhouse-ingest` is one more sequencer, next to the explorer pipeline on the galexie host:

| | explorer pipeline (galexie host) | clickhouse pipeline (this host) |
|---|---|---|
| `Pipeline` | `explorer` | `clickhouse` |
| process | `explorer.Processor`: the ledger row and the state | `explorer.HistoryProcessor`: the ledger row and every history row |
| sink | PostgreSQL | ClickHouse ([`worker/clickhouse/`](../worker/clickhouse/)) |
| sources | lake → archive → AWS lake | AWS lake only |
| tip | the lake's `/v1/tip` | the AWS lake's newest ledger, listed every 5 s (`tip_poll`); the same listing is the network tip |
| unit | `stellar-ingest.service` | `stellar-clickhouse-ingest.service` |

The rest is the same: batches of at most 500 ledgers and 512 MiB; halts on bad data and retries on transient errors; the `ingest_*` metrics, labelled `pipeline="clickhouse"`. If the AWS lake can't be read for `lake_grace` (2 minutes), it alerts (`lake_unavailable`) and keeps retrying: there is no other source, and nothing is lost while it waits.

Its configuration, [`files/worker.toml`](files/worker.toml), has a `[clickhouse]` section and neither `[lake]` nor `[postgres]`.

### 7.2 A batch

1. **Guard:** the batch's first ledger must follow the cursor and link to its hash; otherwise the pipeline halts.
2. **One columnar `INSERT` per table** for the whole batch, over the native protocol, in a fixed order: `ledgers`, `transactions`, `operations`, `events`, `token_transfers` (which fills `transfers_by_account`), `tx_by_hash`, `account_activity`, `contract_calls`, `contract_creations`. Live, a batch is one ledger every ~5 s; ClickHouse merges the small parts.
3. **The cursor:** a row `(pipeline, sequence, hash, time)` in `ingest_state`, a `ReplacingMergeTree`. The current cursor is the row with the highest sequence.

Connection loss, timeouts and a restarting server are transient: the batch is retried. Type or conversion errors halt the pipeline.

### 7.3 Exactly once, without transactions

ClickHouse has no transaction across tables, so a crash between step 2 and step 3 leaves rows **above the cursor**. The rule: **rows above the cursor are never trusted.**

- **On start, and after a failed commit,** the sink deletes them from every table (`DELETE FROM stellar.<t> WHERE ledger_sequence > <cursor>`), then resumes from the cursor.
- **Why that's safe:** this one unit is the only writer above the cursor.
- **Not the usual alternatives:** `ReplacingMergeTree` removes duplicates only when parts merge, so every query would need `FINAL`; insert deduplication only catches identical blocks, and after a restart the batches may be cut differently.

### 7.4 Where it starts

`worker clickhouse-init` (step `init`) seeds the cursor once: by default at the archive's oldest ledger − 1, with that ledger's previous hash: go-live (`archive_origin`). `--at <seq>` starts at any archived ledger. Every later start follows the cursor. History before go-live isn't loaded (§11).

### 7.5 Checking it

```sh
worker clickhouse-check                                 # the last 10,000 ledgers up to the cursor
worker clickhouse-check --from 64806592 --to 64808752   # a range of your choice
curl -s localhost:9103/readyz; curl -s localhost:9103/metrics | grep -E '^ingest_(cursor|lag_ledgers)|^ingest_lake_tip'
```

`clickhouse-check` compares the range's transactions and operations with the counts in the ledgers' own headers, and counts the rows above the cursor in every table, which must be none while the pipeline runs. The metrics give the cursor against the AWS lake's tip (`ingest_lake_tip`); `lab clickhouse check` shows the same.

---

## 8. Public read-only SQL

With `clickhouse_hostname` set on the psql host (for example `clickhouse.api.example.com`), its `clickhouse` step adds a Caddy site ([`psql/files/clickhouse.caddy`](../psql/files/clickhouse.caddy)). ClickHouse itself stays on the private network.

- **Anyone may run SELECTs:** `GET /?query=…` or a POST body, and ClickHouse's SQL page at `/play`; `/ping` for health. Everything else of ClickHouse's HTTP interface is 404.
- **The frontend's backend logs in as `codemelt`** with the password `astrolabe` (basic auth; in plain in [`clickhouse.caddy`](../psql/files/clickhouse.caddy), the same login as on the API). Caddy matches the whole header, drops it and logs in as `stellar_api` for it, so `stellar_api`'s profile and quota apply (§5.3) and its password never leaves the two hosts. Its budget in Caddy is 2,000 requests in any 10 s, from wherever it calls. Any other basic auth counts as anonymous.
- **Everyone else: Caddy logs in, never the client.** It drops `Authorization`, cookies, the `X-ClickHouse-*` headers, the trace context headers (`traceparent`, `tracestate`) and the `user`, `password` and `quota_key` parameters (for `codemelt` too), then logs in as `stellar_public` (§5.3) over 8443, trusting only lab's CA. Basic auth as any other user is dropped the same way.
- **Limits:** the profile and quota of `stellar_public`, separate from Grafana's and `stellar_api`'s, plus Caddy's 30 requests per 10 s per client IP.

**How the backend's limits are sized** (the frontend's page queries replayed as `stellar_api` from the psql host, page-like: transactions by hash, their operations, account histories including the 20 busiest accounts, assets, top holders, statistics):

| Concurrent | Queries/s | p50 | p99 |
|---|---|---|---|
| 1 | 118 | 6.6 ms | 35 ms |
| 4 | 703 | 3.9 ms | 21 ms |
| 8 | 1,342 | 4.3 ms | 19 ms |
| 16 | 1,362 | 9.3 ms | 37 ms |
| 24 | 1,225 | 13.6 ms | 54 ms; the rest refused (20 at once was the cap) |

- The pipeline stayed within 2 ledgers of the tip throughout. A single query took at most 0.33 s, 53 MB and 50 rows.
- So: 10 s and 1 GB a query, 4 GB in all, 16 at once (past 8 only latency grows), and Caddy's 2,000 per 10 s, about 15% of what the server answers.
- **No errors limit below the queries limit.** Refused queries count as errors (too many at once, a quota, bad SQL), so a burst can use up a low errors limit and refuse every query for the rest of the hour: the 24-at-once run produced 15,405 errors in 15 s. The `api` quota allows Caddy's budget all hour (720,000 queries), so Caddy's 429s, gone in seconds, always come first; the public quota's errors limit equals its queries limit for the same reason.

Checked from outside: every credential trick (`?user=default`, basic auth as `stellar_writer`, `X-ClickHouse-User: default`) still runs as `stellar_public`; `CREATE`, `INSERT`, reading `system.query_log` and `?readonly=0` are refused; a runaway query stops at 10 s.

**For clients:** parameters keep values out of the SQL, and `FORMAT JSON` returns the rows with their types. 64- and 128-bit integers come as strings: the `public` and `api` profiles set `output_format_json_quote_64bit_integers` (ClickHouse's default sends numbers, which JavaScript rounds). ClickHouse answers CORS, so browsers can call it from any site.

```ts
const url = new URL('https://clickhouse.example.com/')
url.searchParams.set('query', 'SELECT sequence, tx_count, closed_at FROM stellar.ledgers WHERE sequence = {seq:UInt32} FORMAT JSON')
url.searchParams.set('param_seq', '64806858')
const { data } = await (await fetch(url)).json()
```

The public quota is shared by everyone; the frontend's backend has its own, as `codemelt`.

---

## 9. Operations

### 9.1 Where things are

| What | Where |
|---|---|
| ClickHouse | data `/var/lib/clickhouse`; configuration `/etc/clickhouse-server/` (`config.d/10-stellar.xml`, `users.d/10-stellar.xml`, `users.d/90-passwords.xml`, `tls/`); logs `/var/log/clickhouse-server/`; unit `clickhouse-server` |
| The pipeline | `/usr/local/bin/worker` → `/usr/local/lib/stellar/worker-<version>`; `/etc/stellar/worker.toml`, `archive.env`, `clickhouse-*.password`; unit `stellar-clickhouse-ingest`; metrics `:9103` |
| The state copy | on the psql host: `stellar-state-copy.timer` → `.service`, `/etc/stellar/worker.toml` there (§10) |
| lab | settings `/etc/stellar/lab-clickhouse.toml`; secrets `/etc/stellar/secrets/`; the bundle `/root/clickhouse-bundle/` |
| SQL as the administrator | `clickhouse-client` as root (logs in as `default`) |

### 9.2 Deploying a new worker

`lab clickhouse` installs the worker shipped next to it, but never switches a running one. Build with a new version (`make dist VERSION=…`), then:

```sh
scp bin/worker root@<clickhouse public IP>:/root/worker-$V
ssh root@<clickhouse public IP>
install -m 0755 /root/worker-$V /usr/local/lib/stellar/worker-$V
ln -sfn /usr/local/lib/stellar/worker-$V /usr/local/bin/worker
worker clickhouse-migrate                   # when the release adds migrations (as root: it reads the owner's password)
systemctl restart stellar-clickhouse-ingest
curl -s localhost:9103/readyz               # ready
```

The restart deletes any rows above the cursor and resumes (§7.3). To go back, point the link at the previous version and restart; a migration stays applied.

### 9.3 Configuration changes and upgrades

- **A configuration file** here or in `worker/clickhouse/`: edit, `make dist`, copy `bin/`, `lab clickhouse --allow-restart`.
- **A system log's TTL:** at its restart, ClickHouse keeps the old table as `system.<name>_0` and creates a new one. Drop the old ones once you've checked the new: `SELECT name FROM system.tables WHERE database = 'system' AND match(name, '_log_[0-9]+$')`, then `DROP TABLE system.<name>_0 SYNC`.
- **Upgrading ClickHouse:** change `chVersion` in [`cli/clickhouse.go`](../cli/clickhouse.go) to a newer LTS, rebuild, `lab clickhouse --allow-restart`. Read the release's backward-incompatible changes first. No upgrade has been done yet.
- **Reboots:** both services start at boot; the pipeline waits for ClickHouse and catches up.

### 9.4 Monitoring

Prometheus on the psql host scrapes three jobs: `clickhouse` (9363), `clickhouse_node` (9100) and `clickhouse_ingest` (9103, every 5 s). The Grafana dashboard *Stellar explorer: ClickHouse* shows the pipeline, the server and the history ([dashboard/README.md](../dashboard/README.md#clickhouse)).

[`files/alerts.yml`](files/alerts.yml): the explorer's history, assets and statistics come from this host, so ClickHouse, its pipeline and its disk page:

| Alert | Severity | When |
|---|---|---|
| `ClickHouseDown`, `ClickHousePipelineDown` | page | ClickHouse's or the pipeline's metrics unreachable for 5 minutes |
| `ClickHousePipelineHalted` | page | the pipeline halted on bad data |
| `ClickHousePipelineBehind` | page | more than 720 ledgers (an hour) behind the network for 15 minutes, while the network advances |
| `ClickHouseTooManyParts` | warning | more than 300 parts in a partition for 15 minutes: inserts outpace merges |
| `ClickHouseDiskUsage`, `ClickHouseDiskCritical`, `ClickHouseDiskFullIn30Days` | warning, page, warning | the root filesystem over 70 %, over 85 %, or full within 30 days at the last week's rate |
| `StateCopyStale`, `StateCopyFailed`, `StateCopyMetricMissing` | warning | no successful state copy for two days, its unit failed, or none ever (§10) |

Day to day: `system.parts` for sizes, `system.mutations` for the recovery deletes, `system.query_log` for slow queries.

### 9.5 Retention

| What | Kept |
|---|---|
| History, statistics, snapshots (`stellar.*`) | **everything**, from go-live on: no TTL. The state copy, `assets` and `top_holders` are replaced by each run |
| System logs | 14 days: `query_log`, `query_thread_log`, `query_views_log`, `query_metric_log`, `part_log`, `trace_log`, `text_log`, `metric_log`, `asynchronous_metric_log`, `processors_profile_log`, `error_log`, `crash_log`, `backup_log`, `background_schedule_pool_log`, `opentelemetry_span_log` |
| | the package's own TTLs: `asynchronous_insert_log` 3 days; `blob_storage_log` and the ZooKeeper logs 30 days |
| | none, and never written here: the S3Queue, Iceberg, Delta Lake and instrumentation logs |
| Server log files | 10 × 100 MB each for the log and the error log |
| journald | capped as on the other hosts |

Every log a client can make ClickHouse write has a TTL: `log_query_threads = 1` writes `query_thread_log`, and a trace context writes `opentelemetry_span_log` (Caddy drops the headers, but a setting can still turn tracing on).

### 9.6 Rebuilding: no backups

Everything but the snapshots comes back from the AWS lake; the archive bucket, kept forever, marks go-live for `init`. A lost server is rebuilt with `lab clickhouse` on a new one (with the old secrets back in `/etc/stellar/secrets/`, or a new bundle for the psql host), which migrates, seeds the cursor at go-live and catches up; the next state copy refills the rankings.

- **How long:** catching up, the pipeline reads and writes about 28 ledgers a second from the AWS lake, so a day of history (17,280 ledgers) takes about 10 minutes, a year about 2.5 days.
- **What is lost:** `asset_snapshots` and `holder_snapshots` before the rebuild. They came from the state as it was each day, which nothing else keeps.
- **Re-ingesting on purpose** (a bug in the history, galexie/README.md §7.4.13): stop the pipeline, `DROP DATABASE stellar SYNC` as `default`, then `lab clickhouse` (it creates the database, migrates, seeds the cursor, starts the pipeline). Keep the snapshots first if they matter: `CREATE TABLE default.holder_snapshots_keep AS stellar.holder_snapshots` and an `INSERT … SELECT` of both, then copy them back after the migrate step.

### 9.7 Runbooks

- **The pipeline halted** (`ClickHousePipelineHalted`): `journalctl -u stellar-clickhouse-ingest` names the ledger and the error. A halt is deterministic: restarting repeats it. Fix the worker, deploy it (§9.2); the restart cleans up above the cursor.
- **The pipeline is behind:** check `/readyz` and `journalctl -u stellar-clickhouse-ingest`. While the AWS lake can't be read the pipeline retries, and it catches up by itself once it can.
- **The disk fills:** more storage (§2: a larger server or volume). Nothing expires, and nothing should be dropped: a dropped month comes back only by a re-ingest (§9.6), and snapshots not at all.
- **Too many parts:** merges are behind. Check `system.merges` and the disk; the batches are already one insert per table.
- **The state copy** (`StateCopy*`): §10.

### 9.8 Credential rotation

Replace `/etc/stellar/secrets/clickhouse-<user>.password` with a new value, then run `lab clickhouse` (the hashes, the worker's files, the bundle) and `systemctl restart stellar-clickhouse-ingest` for the writer's. For the reader's, the public's or `stellar_jobs`', copy the bundle to the psql host again and run `lab psql` and `lab grafana`. For `stellar_api`'s, copy the bundle to the psql host and run `lab psql`: Caddy logs in with it, and the frontend's login, `codemelt`, stays the same. Not tried yet.

---

## 10. The state copy, assets and top holders

`worker state-copy` ([`worker/statecopy/`](../worker/statecopy/)) runs on the psql host, where PostgreSQL is local, once a day at 00:15 UTC (`stellar-state-copy.timer`, installed by `lab psql`'s `jobs` step). One run:

1. **Reads the state** over the Unix socket as `state_reader` (peer authentication for the OS user `stellar`), in one read-only `REPEATABLE READ` transaction, so the copy is the state at one ledger: the explorer cursor read in it. Accounts, trustlines (not pool shares), Stellar Asset Contract balances and instances, claimable balances and liquidity pools.
2. **Loads it into `<table>_new`** copies of the `state_*` tables, as `stellar_jobs` over native TLS (9440), 200,000 rows per insert.
3. **Computes `assets_new` and `top_holders_new`** in ClickHouse from them (SQL in [`worker/statecopy/rankings.go`](../worker/statecopy/rankings.go)): classic assets from trustlines, contracts, claimable balances and pool reserves, with the issuer's flags and home domain; XLM with the ledger's `total_coins`; Soroban-only tokens (SEP-41 contracts without a Stellar Asset Contract) from their transfers since go-live, `complete` only when created since; then the top 500 of each asset and the top 1,000 of XLM, ties to the lower address.
4. **Swaps every table in** with `EXCHANGE TABLES`, one atomic swap per table within a second, so a reader sees a whole copy, and drops the previous one.
5. **With `--snapshot`** (the timer's), appends `assets` and `top_holders` to `asset_snapshots` and `holder_snapshots`, dated the day before the run (UTC). A second run on the same day replaces that date's rows.
6. **Records the run** in `state_copy_runs` (ledger, rows, snapshot date); the unit's `ExecStartPost` writes the success marker for node_exporter (`StateCopyStale`).

A run takes a minute or two (accounts and trustlines: ~36 s of reading, ~23 s of loading, measured). A failed one leaves the last copy in place; the next one starts over, dropping any `_new` tables left behind.

```sh
# on the psql host
systemctl list-timers stellar-state-copy.timer
systemctl start stellar-state-copy        # by hand: the whole run, snapshots included
journalctl -u stellar-state-copy          # "state copied as of ledger … in …: map[…]"
runuser -u stellar -- worker state-copy   # without the snapshots
```

```sql
-- here: the last runs
SELECT started_at, finished_at - started_at AS seconds, ledger, closed_at, rows, snapshot FROM stellar.state_copy_runs ORDER BY started_at DESC LIMIT 5;
```

**Freshness:** the asset list, supplies and top holders are as of the last run (`assets.ledger`, at most a day old); `daily_stats` follows the pipeline within 5 minutes. Running the timer hourly instead is a change of its `OnCalendar` (keep `--snapshot` daily).

---

## 11. Limitations

- **History before go-live** isn't loaded: the pipeline starts at go-live. A backfill would read the AWS lake from an earlier ledger.
- **Deploying a new worker** here is by hand (§9.2); the galexie host has a deploy script (`deploy-ingest.sh`).

---

## 12. Development

The worker's ClickHouse code is [`worker/clickhouse/`](../worker/clickhouse/). Its unit tests run on real ledgers without a server. The integration tests run against a real ClickHouse given by `TEST_CLICKHOUSE_URL`, in a database of their own that they drop afterwards; without the variable they are skipped. Nothing is installed on the workstation: they use this host through an SSH tunnel.

```sh
ssh -N -L 19000:127.0.0.1:9000 root@<clickhouse public IP> &
P=$(ssh root@<clickhouse public IP> cat /etc/stellar/secrets/clickhouse-default.password)
cd worker && TEST_CLICKHOUSE_URL="clickhouse://default:$P@127.0.0.1:19000/default" go test ./clickhouse/...
```

`worker/statecopy`'s integration test needs both `TEST_CLICKHOUSE_URL` and `TEST_DATABASE_URL` (a local PostgreSQL, as for the worker's other tests).

`lab`'s code for this host is [`cli/clickhouse.go`](../cli/clickhouse.go), and for the psql side [`cli/psql_clickhouse.go`](../cli/psql_clickhouse.go), [`cli/psql_jobs.go`](../cli/psql_jobs.go) and [`cli/grafana_clickhouse.go`](../cli/grafana_clickhouse.go).

---

## 13. Files

| File | Installed as |
|---|---|
| [`files/clickhouse.sources`](files/clickhouse.sources) | `/etc/apt/sources.list.d/clickhouse.sources` (the key: `/usr/share/keyrings/clickhouse-keyring.asc`) |
| [`files/stellar-config.xml`](files/stellar-config.xml) | `/etc/clickhouse-server/config.d/10-stellar.xml` (root:clickhouse 0640) |
| [`files/stellar-users.xml`](files/stellar-users.xml) | `/etc/clickhouse-server/users.d/10-stellar.xml` (root:clickhouse 0640) |
| [`files/nftables.conf`](files/nftables.conf) | `/etc/nftables.conf` |
| [`files/vswitch.yaml`](files/vswitch.yaml) | `/etc/netplan/60-vswitch.yaml`, with a VLAN only |
| [`files/sshd-hardening.conf`](files/sshd-hardening.conf), [`journald.conf`](files/journald.conf), [`needrestart.conf`](files/needrestart.conf) | as on the psql host |
| [`files/worker.toml`](files/worker.toml) | `/etc/stellar/worker.toml` (root:stellar 0640) |
| [`files/archive.env.example`](files/archive.env.example) | `/etc/stellar/archive.env` with the key (root:stellar 0640) |
| [`files/stellar-clickhouse-ingest.service`](files/stellar-clickhouse-ingest.service) | `/etc/systemd/system/` |
| [`files/alerts.yml`](files/alerts.yml) | on the psql host: `/etc/prometheus/rules/clickhouse-alerts.yml` |
| `files.go` | (not installed) embeds `files/` for `lab` |
