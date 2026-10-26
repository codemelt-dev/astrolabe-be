# Dashboard: Grafana

Grafana runs on the psql host and visualises the metrics collected by the psql host's Prometheus, which scrapes every host ([`psql/README.md`](../psql/README.md#monitoring)). When the clickhouse host is present, Grafana also queries ClickHouse directly. `lab grafana` installs and configures Grafana from the files in this folder; `lab grafana check` reports its state without changing anything.

## Dashboards

| Dashboard | Shows |
|---|---|
| **Stellar explorer: pipeline** ([`pipeline.json`](pipeline.json)) | the galexie host: network tip, ingest cursor and the lag of Galexie, the lake, ingest and the archive; ledgers per second; ledger latency and PostgreSQL commit time; lake write and fsync times; disk use; Galexie and watchdog restarts; pipeline alerts; scrape targets |
| **Stellar explorer: database and API** ([`psql.json`](psql.json)) | the psql host: requests by status, response times and rate-limited requests (Caddy); connection pool and timeouts (PostgREST); database size, connections, transactions, cache hit ratio, WAL archiving and time since the last full backup (PostgreSQL, pgBackRest); CPU, memory, disk and network |
| **Stellar explorer: ClickHouse** ([`clickhouse.json`](clickhouse.json)), when the clickhouse host is present | the ClickHouse pipeline's cursor, lag and throughput; ClickHouse queries, memory, parts and disk (Prometheus); transactions, fees and token transfers per hour, operations by type, the most transferred tokens and the busiest contracts (SQL on ClickHouse) |

The panels only use metric names exported by the worker, the exporters, PostgREST and Caddy, the same ones the alert rules in `galexie/deploy/alerts.yml` and `psql/files/alerts.yml` use. A panel showing *No data* usually means its scrape target is down (see the *Scrape targets up* panel).

## Setup

Grafana reads the psql host's Prometheus, so monitoring must be enabled there first (`monitoring_enabled = "yes"` in `/etc/stellar/lab-psql.toml`, then `lab psql`).

```sh
/root/bin/lab grafana          # install, configure and start
/root/bin/lab grafana check    # read-only: installation, configuration, data sources, dashboards
```

`lab grafana`:

- installs Grafana OSS 13.2.3 from its release `.deb`, verified against a pinned sha256;
- writes [`grafana.ini`](grafana.ini) (listens on `127.0.0.1:3002`, no sign-up, no anonymous access, no telemetry, logs to the journal), the Prometheus data source ([`datasources.yaml`](datasources.yaml)) and the dashboard provider ([`dashboards.yaml`](dashboards.yaml)), and copies the dashboards into `/etc/grafana/dashboards/stellar-explorer/`;
- sets the admin password;
- enables and starts `grafana-server`, and restarts it when its configuration or version changes.

Once Grafana is installed, every `lab psql` run also keeps it in the documented state, and `lab psql check` reports on it.

### ClickHouse data source

When the psql host's `clickhouse` step is enabled ([`clickhouse/README.md`](../clickhouse/README.md#integration-with-the-psql-host)), `lab grafana` also:

- installs the `grafana-clickhouse-datasource` plugin 4.22.1, verified against a pinned sha256;
- provisions the data source ([`clickhouse-datasource.yaml`](clickhouse-datasource.yaml)): ClickHouse's native protocol over TLS to the clickhouse host's private IP, port 9440, trusting only the clickhouse host's CA, as the read-only user `stellar_reader`, database `stellar`;
- installs [`clickhouse.json`](clickhouse.json).

Grafana viewers can send arbitrary queries through a data source via Grafana's query API, so ClickHouse's `reader` profile bounds what they can do: read-only, 30 s and 4 GB per query, with its own hourly quota ([`clickhouse/README.md`](../clickhouse/README.md#users)).

## Access

Grafana listens on loopback only. It is reached either through an SSH tunnel:

```sh
ssh -L 3002:127.0.0.1:3002 root@<psql host>
# then open http://localhost:3002
```

or under a public hostname. Setting `grafana_hostname` in `/etc/stellar/lab-psql.toml` and running `lab grafana` adds a Caddy site for that name ([`psql/files/grafana.caddy`](../psql/files/grafana.caddy)) and gives Grafana its public URL ([`grafana-public.conf`](grafana-public.conf)). Caddy then terminates TLS, proxies to Grafana on loopback, and limits login attempts to 10 per minute per client IP, in addition to Grafana's own lockout after repeated failures. Setting `grafana_hostname = "none"` removes the public site again.

Viewer accounts (not the admin login) should be used for people who only look at dashboards: a Viewer cannot edit dashboards or add data sources.

## Changing dashboards and configuration

- **A dashboard:** edit its JSON here (or export it from the UI with *Export → JSON*), rebuild with `make dist`, copy `bin/` to the host and run `lab grafana`. Grafana rescans the folder every 30 s. Provisioned dashboards cannot be saved from the UI (`allowUiUpdates: false`), so the repository stays the source of truth.
- **A new dashboard:** add a `*.json` file with a unique `uid`, the data source `{"type": "prometheus", "uid": "prometheus"}` and the tag `stellar-explorer`. Only `clickhouse.json` may also use `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}` (checked by `cli/cli_test.go`).
- **A ClickHouse panel:** test its SQL as `stellar_reader` first. In ClickHouse a column alias is visible in the whole query, so an alias that reuses a column name (`countIf(successful) AS successful`) changes what the rest of the query reads; aliases need their own names.
- **Upgrades:** Grafana's version and sha256 are `grafanaVersion` and `grafanaSHA256` in `cli/grafana.go`; the plugin's are `chPluginVersion` and `chPluginSHA256` in `cli/grafana_clickhouse.go`.

## Files

| File | Installed as |
|---|---|
| `grafana.ini` | `/etc/grafana/grafana.ini` |
| `grafana-public.conf` | `/etc/systemd/system/grafana-server.service.d/20-public.conf`, only with a public hostname |
| `datasources.yaml` | `/etc/grafana/provisioning/datasources/stellar-explorer.yaml` |
| `dashboards.yaml` | `/etc/grafana/provisioning/dashboards/stellar-explorer.yaml` |
| `pipeline.json`, `psql.json` | `/etc/grafana/dashboards/stellar-explorer/` |
| `clickhouse-datasource.yaml` | `/etc/grafana/provisioning/datasources/stellar-clickhouse.yaml`, with the clickhouse host |
| `clickhouse.json` | `/etc/grafana/dashboards/stellar-explorer/`, with the clickhouse host |
| `files.go` | not installed: embeds this folder into `lab` |
