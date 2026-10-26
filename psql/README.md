# psql host

The psql host runs the explorer's **current-state database and public REST API**: PostgreSQL 18 with the `explorer` database, PostgREST and Caddy serving a read-only API, pgBackRest backups, and (optionally) Prometheus, Alertmanager and Grafana monitoring every host. It also runs the daily state copy into ClickHouse.

The galexie host writes every ledger into this database over the private network; this host serves it to the internet. `lab psql` sets the host up; `lab psql check` reports its state and health without changing anything.

## Contents

1. [What runs here](#what-runs-here)
2. [Design decisions](#design-decisions)
3. [The REST API](#the-rest-api)
4. [PostgreSQL](#postgresql)
5. [Caddy: the public edge](#caddy-the-public-edge)
6. [Backups](#backups)
7. [Monitoring](#monitoring)
8. [Setup with `lab psql`](#setup-with-lab-psql)
9. [Verifying the API](#verifying-the-api)
10. [Operations](#operations)
11. [Development](#development)

---

## What runs here

```text
                 internet: HTTPS 443/tcp, HTTP/3 443/udp, 80/tcp (ACME and redirect only)
                                      │
┌─ psql host ──────────────────────────┼───────────────────────────────────────────────────┐
│  Caddy               the only public listener: TLS, read-only methods, rate limits       │
│                                      │ 127.0.0.1:3000                                    │
│  PostgREST 16.4      REST over the api schema                                            │
│                                      │ Unix socket, peer auth: authenticator → web_anon  │
│  PostgreSQL 18       database explorer: schema explorer (worker), schema api (api.sql)   │
│                                      ▲ TLS 5432, from the galexie host only              │
│  pgBackRest          continuous WAL archiving, weekly full backup → backup bucket        │
│  state copy          daily: current state → ClickHouse on the clickhouse host (TLS 9440) │
│  Prometheus, Alertmanager, Grafana, exporters: loopback only                             │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                      │ private network
              galexie host: worker ingest and migrate (writes), metrics endpoints (scraped)
```

| Component | Purpose |
|---|---|
| **PostgreSQL 18** | one database, `explorer`. The worker owns schema `explorer` and `public.ingest_state` through its migrations; this folder owns the database, the roles ([`roles.sql`](roles.sql)), the grants ([`grants.sql`](grants.sql)) and the public `api` schema ([`api.sql`](api.sql)). |
| **PostgREST** | turns the `api` views into a REST API, connecting over the Unix socket. |
| **Caddy** | HTTPS with automatic Let's Encrypt certificates, read-only methods, per-client rate limits, security and cache headers. A custom build with the rate-limit module ([`caddy/`](caddy/)). |
| **pgBackRest** | continuous WAL archiving and a weekly full backup to an S3-compatible bucket in another location, encrypted client-side. |
| **Prometheus, Alertmanager** (optional) | scrape every host and send e-mail; an external dead-man's switch alerts when they stop. |
| **Grafana** (optional) | dashboards ([`dashboard/README.md`](../dashboard/README.md)). |
| **`worker state-copy`** | once a day, copies the current state into ClickHouse, where the asset list, supplies and top holders are computed ([`clickhouse/README.md`](../clickhouse/README.md#state-copy-assets-and-top-holders)). It reads as `state_reader`, read-only. |

### Network traffic

| From | To | Port | Purpose |
|---|---|---|---|
| anyone | this host | 443/tcp, 443/udp, 80/tcp | the API (80 only redirects and answers ACME challenges) |
| admin IPs | this host | 22 | SSH |
| galexie host | this host (private) | 5432, TLS | `ingest_writer` (ingest, bootstrap, reindex), `explorer_owner` (`worker migrate`) |
| this host | galexie host, clickhouse host (private) | metrics ports | Prometheus |
| this host | clickhouse host (private) | 8443, 9440, TLS | public SQL proxy, Grafana, state copy |
| this host | backup bucket | 443 | pgBackRest |

## Design decisions

- **PostgREST and Caddy share the database host.** The `api` schema belongs to the database, and PostgREST is a stateless translator. Next to PostgreSQL it connects over the Unix socket with peer authentication, which needs no password, certificate or network rule, and adds no network round trip. It also means one server fewer to run. If API traffic outgrows the host, Caddy and PostgREST can move to their own server with a configuration change; the API itself does not change.
- **Containment of the public-facing database host:**
  - nftables drops everything except 80/443, SSH from admin IPs, and 5432 from the galexie host on the private interface;
  - PostgreSQL listens only on `localhost` and the private IP; PostgREST, Prometheus, Alertmanager, Grafana and the exporters listen only on loopback;
  - Caddy is the only public listener;
  - the anonymous role can only read the `api` views, for at most 2 s per statement, 200 rows per response.
- **Current state only.** PostgreSQL holds the current ledger state and one row per ledger, so it grows with the network's state (a few GB a year), not with its traffic. History lives in ClickHouse at about a tenth of the size.
- **An outage of this host** takes the API down, but nothing upstream is lost: ingest on the galexie host retries while Galexie and the lake keep buffering ledgers. Maintenance restarts are safe.

## The REST API

[`api.sql`](api.sql) is the API contract: read-only views over the worker's tables, plus the indexes they need. Nothing else is exposed. The current version, **API version 2**, serves current state and ledgers; transactions, operations, events, statistics and rankings come from ClickHouse ([`clickhouse/README.md`](../clickhouse/README.md#public-read-only-sql)).

### Contents

| View | Rows | Key | Indexed lookups |
|---|---|---|---|
| `ledgers` | one per ledger, from the bootstrap checkpoint on | `sequence` | `sequence`, `hash`, `closed_at` |
| `accounts` | current state | `account_id` | `account_id` |
| `trustlines` | current state | `(account_id, asset)` | by account, by asset |
| `data_entries` | an account's name/value pairs | `(account_id, name)` | by account |
| `offers` | open DEX offers (with `price`) | `offer_id` | by seller, by pair and price |
| `claimable_balances` | current state | `balance_id` | by claimant, by asset |
| `liquidity_pools` | current state | `pool_id` | by id, by pair |
| `contracts` | one per contract instance (with TTL) | `contract_id` | by id, `wasm_hash`, Stellar Asset Contract asset |
| `contract_data` | current state (with TTL) | `key_hash` | by key, by contract |
| `sac_balances` | Stellar Asset Contract balances | `key_hash` | by holder |
| `contract_code` | current state (with TTL and decoded `spec`) | `key_hash` | by key, by code hash |
| `contract_wasm` | uploaded Wasm, base64 | `code_hash` | by code hash |
| `status` | one row: ingest progress | | |

Computed relationships allow embedding: `accounts → trustlines, sac_balances, data_entries, offers, claimable_balances`; `contracts → contract_code`. The function `rpc/order_book?base=…&counter=…&depth=20` aggregates a pair's offers by price level.

**Value representations:**

| Values | JSON | Filtering |
|---|---|---|
| hashes and keys | lower-case hex | hex, any case |
| XDR and bytes | base64 | not filterable |
| 64-bit integers that can exceed 2^53 (amounts, sequence numbers, offer ids) | decimal string | plain decimal |
| ledger sequences, counts, flags | numbers | numbers |
| `closed_at` | ISO 8601, UTC | `?closed_at=gte.2026-10-01T00:00:00Z` |
| ScVal (`key`, `val` of contract data) | the worker's JSON, e.g. `{"symbol":"transfer"}` | JSON equality |
| `spec` | the Wasm's `contractspecv0` entries as JSON | JSON containment |

These representations are implemented with PostgreSQL domains in a private `api_types` schema and PostgREST's domain representations: filters and indexes see the base types, responses see the JSON forms.

### Querying

Every list request must filter or order on an indexed key, otherwise it hits the 2 s statement timeout (HTTP 500, code `57014`). Responses carry at most 200 rows. Paging uses keyset filters, not offsets.

| Need | Request |
|---|---|
| ingest status | `/status` |
| latest ledgers | `/ledgers?order=sequence.desc&limit=20`, next page `&sequence=lt.<last>` |
| a ledger | `/ledgers?sequence=eq.60000000` or `/ledgers?hash=eq.<hex>` |
| the ledger at a time | `/ledgers?closed_at=lte.2026-10-01T00:00:00Z&order=closed_at.desc&limit=1` |
| an account with everything it holds | `/accounts?account_id=eq.G…&select=*,trustlines(*),sac_balances(*),data_entries(*),offers(*),claimable_balances(*)` |
| a pair's order book | `/rpc/order_book?base=native&counter=USDC:GA5Z…&depth=20` |
| an account's claimable balances | `/claimable_balances?claimants=cs.[{"destination":"G…"}]` |
| a pair's pools | `/liquidity_pools?asset_a=eq.native&asset_b=eq.USDC:GA5Z…` |
| a contract with its functions | `/contracts?contract_id=eq.C…&select=*,contract_code(spec)` |
| a contract's data | `/contract_data?contract_id=eq.C…&order=key_hash&limit=50`, next page `&key_hash=gt.<hex>` |
| an asset's holders | `/trustlines?asset=eq.USDC:GA5Z…&order=account_id&limit=50` |

`Prefer: count=exact` on large views runs into the timeout; `count=planned` or `count=estimated` work. The OpenAPI document at `/` lists every view and column. TypeScript types for the API are in [`generated/`](../generated/README.md).

### How the API is applied

The `api` step of `lab psql` waits until the worker's migrations reach the version `api.sql` requires, then runs `SET ROLE explorer_owner`, [`grants.sql`](grants.sql) and [`api.sql`](api.sql) in one session over the Unix socket, and records their sha256 and the schema version in `COMMENT ON SCHEMA api`. It re-applies them whenever the files change, a migration raises the schema version, or views are missing. Indexes are created first (`CREATE INDEX IF NOT EXISTS`); the views are replaced in one transaction, so a failure leaves the previous API in place. PostgREST reloads its schema cache on `NOTIFY pgrst`.

### Changing the API

- Additive changes (new view, new trailing column, new index) keep the version; breaking changes bump it in `api.sql`, in `api.status` and in the changelog below, coordinated with the frontend.
- View columns are always listed explicitly, so a worker migration never changes the API by accident.
- A new index on a populated table is first built with `CREATE INDEX CONCURRENTLY` (a plain `CREATE INDEX` blocks ingest), then added to `api.sql` as `CREATE INDEX IF NOT EXISTS`.
- A migration that drops or changes a column a view uses needs the new `api.sql` applied first.
- After a change, regenerate the TypeScript types (`make types`).

| API version | Changes |
|---|---|
| 1 | ledgers, transactions, operations, events, contract events, accounts, trustlines, contract data, SAC balances, contract code, status |
| 2 | current state only: history moved to ClickHouse. New: `data_entries`, `offers` and `rpc/order_book`, `claimable_balances`, `liquidity_pools`, `contracts`, `contract_code.spec`, `contract_wasm`. `ledgers` starts at the bootstrap checkpoint. |

## PostgreSQL

### Cluster and TLS

- **Packages** come from the PGDG repository. Unattended upgrades only install Ubuntu's own security updates, so PostgreSQL is never upgraded unattended.
- **Cluster** `18/main`, created with `--locale-provider=builtin` (code-point sort order, so an OS upgrade can never change index order) and `--data-checksums`.
- **Listening:** `localhost` and the private IP only ([`files/10-listen-tls.conf`](files/10-listen-tls.conf)). `net.ipv4.ip_nonlocal_bind` and a `network-online` drop-in make the private IP reliable at boot.
- **TLS:** `lab psql` keeps a small CA on the host (ECDSA P-256, 10 years) and issues the server certificate for the private IP, renewing it automatically 30 days before expiry. The galexie host trusts this CA and connects with `sslmode=verify-full`, TLS 1.3 only.

### Client authentication ([`files/pg_hba.conf`](files/pg_hba.conf))

| Rule | Who |
|---|---|
| `local all postgres peer` | administration, pgBackRest |
| `local explorer authenticator peer map=postgrest` | PostgREST (OS user `postgrest`) |
| `local all postgres_exporter peer map=exporter` | the metrics exporter |
| `local explorer state_reader peer map=statecopy` | `worker state-copy` (OS user `stellar`) |
| `hostssl explorer ingest_writer,explorer_owner <galexie host>/32 scram-sha-256` | the galexie host |
| everything else | rejected |

### Roles

| Role | Login | Purpose |
|---|---|---|
| `explorer_owner` | password, galexie host only | owns the database, schemas and objects; `worker migrate` and `api.sql` run as it |
| `ingest_writer` | password, galexie host only | `worker ingest`, `bootstrap`, `reindex`: DML and `TRUNCATE` on every table via default privileges |
| `authenticator` | peer, local only | PostgREST's login; switches to `web_anon` per request |
| `web_anon` | none | the anonymous API role: `SELECT` on `api` views only, `statement_timeout = 2s` |
| `postgres_exporter` | peer, local only | monitoring (`pg_monitor`) |
| `state_reader` | peer, local only | `worker state-copy`: `SELECT` on the copied tables, read-only transactions |

[`roles.sql`](roles.sql) is idempotent and applied by the `database` step; [`grants.sql`](grants.sql) is re-applied after every migration. Role passwords are generated by `lab psql`; only their SCRAM-SHA-256 verifiers (computed locally) are sent to the server, so plaintext never reaches server logs.

### Tuning

[`files/20-tuning.conf`](files/20-tuning.conf) shows the values for the reference server (128 GB RAM, 16 cores). `lab psql` recomputes them from the host's RAM, cores and disk:

| Setting | Computed as |
|---|---|
| `shared_buffers` | 25 % of RAM |
| `effective_cache_size` | 70 % of RAM, at most RAM − 4 GB |
| `work_mem`, `maintenance_work_mem`, `autovacuum_work_mem` | RAM / 2048, RAM / 64, RAM / 128, within bounds |
| `io_workers`, parallel workers | from the core count |
| `max_wal_size` | 1/16 of the data volume (2–64 GB) |
| `vm.nr_hugepages` | `shared_buffers` in 2 MB pages plus a margin |

Fixed settings: WAL compression and checkpoint tuning for ingest throughput, aggressive autovacuum for the update-heavy state tables, `jit = off` for short API queries, `pg_stat_statements`. `synchronous_commit` stays on: each ledger commit is the pipeline's point of truth.

## Caddy: the public edge

The [Caddyfile](files/Caddyfile):

- automatic HTTPS (HTTP-01 or TLS-ALPN-01) and HTTP/3;
- `405` for any method other than `GET`, `HEAD`, `OPTIONS`; request bodies limited to 1 KB;
- `Authorization` and `Cookie` headers dropped, so PostgREST never sees credentials;
- **100 requests per client IP in any 10 s** (IPv6 per /64), then `429`;
- a separate budget of 5,000 requests per 10 s for the frontend's backend, which authenticates with its own HTTP basic-auth login (a single backend serving every visitor would otherwise hit the per-IP limit; PostgREST serves about 5,500 requests per second on this hardware with ingest running);
- security headers (HSTS, `nosniff`, `default-src 'none'`);
- caching: `public, max-age=86400, immutable` on non-empty responses pinned to one ledger or hash, `max-age=5` on everything else;
- JSON error bodies with CORS headers for Caddy's own errors (429, 413, 502);
- access log in `/var/log/caddy/api.log` (rotated, 14 days).

When `clickhouse_hostname` or `grafana_hostname` is set, Caddy also serves the public ClickHouse SQL endpoint ([`files/clickhouse.caddy`](files/clickhouse.caddy)) and Grafana ([`files/grafana.caddy`](files/grafana.caddy)).

`{client_ip}` is the TCP peer, because nothing sits in front of Caddy. If a CDN or load balancer is ever added, `trusted_proxies` must be set, or every visitor shares the proxy's rate limit.

## Backups

pgBackRest archives every WAL segment asynchronously (`archive_timeout = 5min`), takes a **full backup every Sunday**, keeps one full backup (up to a week of point-in-time recovery), and encrypts everything client-side with a generated passphrase. A daily differential backup timer is installed but disabled; enabling it shortens restores.

The backup bucket is S3-compatible object storage in a different location from the server, with a key dedicated to this bucket (it must be able to delete, to expire old backups).

Backups matter even though every ledger is archived: without them the current-state tables can only be rebuilt with a fresh `worker bootstrap`, with the API down meanwhile.

```sh
runuser -u postgres -- pgbackrest --stanza=explorer info          # backups, sizes, WAL range
systemctl start pgbackrest-backup@full.service                    # a full backup now
runuser -u postgres -- psql -c 'SELECT * FROM pg_stat_archiver'   # archiving status
```

**Restore drill.** A restore into a second cluster proves the backups, the WAL archive and the passphrase, and measures the recovery time:

```sh
pg_createcluster --locale=C.UTF-8 --port=5433 18 drill -- --locale-provider=builtin
pg_conftool 18 drill set huge_pages off
pg_conftool 18 drill set max_worker_processes 32
pg_conftool 18 drill set max_locks_per_transaction 256
rm -rf /var/lib/postgresql/18/drill && install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/18/drill
time runuser -u postgres -- pgbackrest --stanza=explorer --pg1-path=/var/lib/postgresql/18/drill --archive-mode=off restore
systemctl start postgresql@18-drill
runuser -u postgres -- psql -p 5433 -d explorer -c "SELECT max(sequence), max(closed_at) FROM explorer.ledgers"
pg_dropcluster --stop 18 drill
```

A drill host must never run `backup`, `stanza-*` or `archive-push`.

## Monitoring

Enabled with `monitoring_enabled = "yes"` in `/etc/stellar/lab-psql.toml`; `lab psql` then asks for the SMTP account, the paging address and the dead-man's-switch URL.

- **Rules:** [`files/alerts.yml`](files/alerts.yml) for this host and the monitoring itself, the galexie host's [`alerts.yml`](../galexie/deploy/alerts.yml), and, with the clickhouse host, [`clickhouse/files/alerts.yml`](../clickhouse/files/alerts.yml).
- **Routing:** `severity: page` alerts go to the paging address (repeated hourly), everything else to the operations address.
- **Dead-man's switch:** an always-firing `Watchdog` alert is sent every minute to an external heartbeat service, which alerts when it stops arriving (host, Prometheus or Alertmanager down).
- **Access:** everything listens on loopback; use an SSH tunnel (`ssh -L 9090:127.0.0.1:9090 -L 9093:127.0.0.1:9093 -L 3002:127.0.0.1:3002 root@<psql host>`).

Main alerts for this host:

| Alert | Meaning |
|---|---|
| `PostgresDown` | the database is not accepting connections |
| `PostgresWalArchivingFailing`, `PostgresWalArchiveStale` | WAL is not reaching the backup bucket; it accumulates in `pg_wal` |
| `PostgresBackupStale`, `PostgresFullBackupStale` | no recent backup |
| `DataDiskUsage`, `DataDiskFullIn30Days` | data volume filling up |
| `RaidDegraded` | a RAID member failed |
| `PostgresLongRunningTransaction`, `PostgresConnectionsHigh` | a long transaction is holding back vacuum, or the pool is near its limit |
| `ApiServerErrors`, `PostgrestPoolTimeouts` | API errors or database pool exhaustion |
| `ApiRateLimitedShare` | a significant share of requests is being rate-limited |
| `StateCopyStale`, `StateCopyFailed` | the daily state copy into ClickHouse is not succeeding |

## Setup with `lab psql`

**Requirements:** Ubuntu 24.04 amd64; a private network shared with the other hosts; an S3-compatible bucket for backups; a DNS name for the API. The production deployment uses a dedicated server with two NVMe drives in software RAID 1 and LVM, with `/var/lib/postgresql` on its own XFS volume so a full database can never fill `/`; the tuning adapts to other sizes.

Build `bin/` with `make dist` ([root README](../README.md#building)), copy it to `/root/bin/` and run, as root:

```sh
/root/bin/lab psql          # checks every step, lists changes, asks once, applies, checks again
/root/bin/lab psql check    # read-only: every step and health probe; exit 0 only if all is ok
```

| # | Step | Does | Health check |
|---|---|---|---|
| 1 | `system` | packages, UTC, key-only SSH, journald cap, needrestart exemption for PostgreSQL | |
| 2 | `network` | optional VLAN interface for the private IP ([`files/vswitch.yaml`](files/vswitch.yaml)) | private IP present; galexie host reachable |
| 3 | `firewall` | [`files/nftables.conf`](files/nftables.conf), validated with `nft -c`; refuses a ruleset that would lock out the current SSH session | ruleset loaded |
| 4 | `postgres` | PGDG repository, PostgreSQL 18, pgBackRest, the cluster, sysctl, configuration, `pg_hba.conf`, CA and certificate | accepting connections; certificate validity; huge pages |
| 5 | `database` | [`roles.sql`](roles.sql), role passwords as SCRAM verifiers, the bundle for the galexie host | |
| 6 | `backups` | pgBackRest configuration, `stanza-create`, `check`, timers, first full backup | last backup age; archive failures |
| 7 | `api` | waits for the worker's migrations, then [`grants.sql`](grants.sql) and [`api.sql`](api.sql) | newest ledger's age; API version |
| 8 | `postgrest` | release download (sha256-verified), user, [config](files/postgrest.conf), [unit](files/postgrest.service) | `/ready` |
| 9 | `caddy` | the Caddy build shipped in `bin/`, [Caddyfile](files/Caddyfile) (validated), [unit](files/caddy.service); waits until a certificate is served | `GET /` through the public name; certificate validity |
| 10 | `monitoring` | optional: exporters, Prometheus, Alertmanager (sha256-verified releases), rules, checked with `promtool` and `amtool` | every target up |
| 11 | `clickhouse` | optional: the clickhouse bundle, the public SQL site, scrape jobs and alerts | `/ping` and a query through Caddy |
| 12 | `jobs` | with the `clickhouse` step: the worker, [`files/worker.toml`](files/worker.toml), the daily [state-copy timer](files/stellar-state-copy.timer) | last copy's age and result |
| 13 | `grafana` | optional, via `lab grafana` ([`dashboard/README.md`](../dashboard/README.md)) | data sources answer |

**Settings** (`/etc/stellar/lab-psql.toml`, asked once): `private_ip` (example `10.0.1.2`), `galexie_ip` (`10.0.0.2`), the private interface and optional VLAN, `admin_ips`, `api_hostname`, `cors_origins`, `ops_email`, the backup bucket's endpoint, region and name, and optionally `grafana_hostname`, `clickhouse_hostname`, `clickhouse_ip`, `monitoring_enabled` and the SMTP settings.

**Secrets** (`/etc/stellar/secrets/`, root only, never shown or logged):

| Secret | Origin | Purpose |
|---|---|---|
| `backup_s3_key`, `backup_s3_secret` | entered | backup bucket |
| `smtp_password`, `deadman_url` | entered (monitoring only) | alerting |
| `explorer_owner.password`, `ingest_writer.password` | generated | role passwords |
| `pgbackrest-cipher-pass` | generated | backup encryption passphrase |
| `pg-ca.key` | generated | the database CA |

Generated secrets must be stored in a password manager after the first run: without `pgbackrest-cipher-pass` no backup can be restored, and without the CA the galexie host rejects a rebuilt server.

**Order of bring-up across hosts:**

1. `lab psql` on this host. Every step completes except `api` (waits for the worker's migrations) and `caddy` (waits for DNS). It writes the database bundle (CA certificate and password files) to `/root/galexie-bundle/`.
2. Point `A`/`AAAA` records for the API name at this host.
3. Copy the bundle to the galexie host and run `lab galexie` there ([`galexie/README.md`](../galexie/README.md#setup-with-lab-galexie)); it runs the migrations and the initial bootstrap.
4. `lab psql` again: the `api` step applies the API. After the bootstrap's bulk load, refresh statistics with `runuser -u postgres -- vacuumdb --dbname=explorer --schema=explorer --analyze --jobs=4`.
5. `lab psql check` here and `lab galexie check` there; reboot both and check again.

Running services are only restarted with `--allow-restart`; otherwise the step reports `! restart pending`. Reloads (PostgreSQL configuration, Caddyfile, Prometheus rules, nftables) are applied automatically.

## Verifying the API

From anywhere:

```sh
curl -s  https://api.example.com/status                                              # newest ledger and its age
curl -sI https://api.example.com/status                                              # 200, HSTS, Cache-Control: max-age=5
curl -s  -o /dev/null -w '%{http_code}\n' -X POST https://api.example.com/ledgers    # 405
curl -s  "https://api.example.com/ledgers?order=sequence.desc&limit=3"               # the three newest ledgers
```

On the host, the grants can be checked directly:

```sql
SELECT c.oid::regclass AS relation,
       has_table_privilege('ingest_writer', c.oid, 'INSERT') AS dml,
       has_table_privilege('ingest_writer', c.oid, 'TRUNCATE') AS truncate
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname = 'explorer' ORDER BY 1;
```

## Operations

| Task | How |
|---|---|
| Change a setting | edit `/etc/stellar/lab-psql.toml`, run `lab psql` |
| Change a file (`files/`, SQL) | change it in the repository, `make dist`, copy `bin/`, run `lab psql`; installed files are never edited by hand |
| PostgreSQL minor upgrade | `apt-get install --only-upgrade postgresql-18 postgresql-client-18 libpq5 postgresql-common pgbackrest` at a quiet time (the package restarts the cluster) |
| PostgreSQL major upgrade | a new `lab` release, `pg_upgradecluster --locale=C.UTF-8 -v 19 -m link 18 main`, pgBackRest `stanza-upgrade`, a new full backup; rehearse on a drill host first |
| PostgREST, Prometheus, Alertmanager upgrade | change version and sha256 in [`cli/psql_web.go`](../cli/psql_web.go), rebuild, `lab psql --allow-restart`; the old version stays installed for rollback |
| Caddy upgrade | bump the pins in [`caddy/go.mod`](caddy/go.mod), rebuild, `lab psql --allow-restart` |
| Rotate a role password | write a new value to the secret file, `ALTER ROLE <role> PASSWORD NULL`, `lab psql --only database`, copy the new bundle to the galexie host and run `lab galexie --allow-restart` |
| Rotate the backup key | delete the two secret files, run `lab psql` (it asks again), `pgbackrest check` |
| Reset the database | with ingest stopped: `DROP SCHEMA explorer CASCADE; DROP TABLE public.ingest_state;`, then `worker migrate`, `lab psql`, `worker bootstrap` on the galexie host, and a full backup |
| Rebuild a lost server | restore the generated secrets into `/etc/stellar/secrets/` first, run `lab psql --skip api`, stop PostgreSQL, `pgbackrest --stanza=explorer --delta restore`, start it, run `lab psql` |

**Common API symptoms:**

| Symptom | Cause |
|---|---|
| `PGRST205` *Could not find the table* | `api.sql` not applied, or PostgREST not reloaded (`NOTIFY pgrst, 'reload schema'`) |
| `500` with code `57014` | the 2 s statement timeout: the request does not use an index |
| `401`, code `42501` | a missing grant; `lab psql` re-applies `api.sql` |
| hashes or amounts as `\x…` or JSON numbers | domain representations not loaded; re-apply `api.sql` |
| CORS failures in the browser | the origin is missing from `cors_origins` |

## Development

This folder holds the SQL, `files/` (every installed file, written with the example values and a header naming its install path), the Caddy build (`caddy/`) and `files.go`, which embeds them into `lab`. The setup code is in [`cli/`](../cli/): `psql.go` (settings and step table), `psql_pg.go` (PostgreSQL, TLS, SCRAM, backups, API), `psql_web.go` (network, firewall, PostgREST, Caddy, monitoring), `grafana.go`, `grafana_clickhouse.go`, `psql_clickhouse.go`, `psql_jobs.go`.

**Tests** (`make test` at the repository root, no database needed): rendering every file with the example values changes nothing and every example value is used; whole-token replacement; the tuning reproduces `20-tuning.conf` for the reference server and expected values for smaller ones; the SCRAM verifier matches PostgreSQL's; the step table's order.

**The SQL against a local PostgreSQL 18** (as superuser):

```sh
psql -X -v ON_ERROR_STOP=1 -d postgres -f roles.sql                  # twice: the second run changes nothing
psql -X -d postgres -c "ALTER ROLE explorer_owner PASSWORD 'x'"
worker migrate --config <worker.toml with [postgres] migrate_dsn = "postgres://explorer_owner:x@localhost/explorer">
(echo 'SET ROLE explorer_owner;'; cat grants.sql api.sql) | psql -X -v ON_ERROR_STOP=1 -d explorer -f -    # twice
psql -X -d explorer -c 'SET ROLE web_anon; SELECT * FROM api.status'
```

**Other checks:** `(cd caddy && go build -o /tmp/caddy .) && /tmp/caddy adapt --config files/Caddyfile` validates the Caddyfile with the real build; `promtool check rules files/alerts.yml ../galexie/deploy/alerts.yml` validates the alert rules.
