# Dashboard: Grafana on the psql host

Grafana with two dashboards over the psql host's Prometheus, which scrapes every host (psql/README.md, monitoring), and a third over ClickHouse once the clickhouse host is on ([below](#clickhouse)). `lab grafana`, run on the psql host, installs and configures it from the files in this folder; `lab grafana check` reports on it.

| Dashboard | Shows |
|---|---|
| **Stellar explorer: pipeline** ([`pipeline.json`](pipeline.json)) | the galexie host: network tip, ingest cursor and the lag of Galexie, the lake, ingest and the archive; ledgers per second; ledger latency and Postgres commit time; lake PUT and fsync times; `/srv/lake` and `/srv/core` use; Galexie and watchdog restarts; pipeline alerts; scrape targets |
| **Stellar explorer: ClickHouse** ([`clickhouse.json`](clickhouse.json)), with the clickhouse host | the clickhouse pipeline's cursor and lag, ledgers committed per second; ClickHouse's queries, memory, active parts and disk (Prometheus); transactions, fees and token transfers per hour, operations by type, the most transferred tokens and the busiest contracts in the time range (SQL on ClickHouse) |
| **Stellar explorer: database and API** ([`psql.json`](psql.json)) | the psql host: requests per status, response times, rate-limited requests (Caddy); the connection pool and its timeouts (PostgREST); database size, connections, transactions, cache hit ratio, WAL archiving, time since the last full backup (PostgreSQL, pgBackRest); CPU, memory, disk and network |

## Set up

Grafana reads only this host's Prometheus, so the `monitoring` step must be on first (`monitoring_enabled = "yes"` in `/etc/stellar/lab-psql.toml`, then `lab psql`).

```sh
/root/bin/lab grafana          # install, configure, start; asks once
/root/bin/lab grafana check    # read-only: installed, configured, running, data source and dashboards
```

`lab grafana`:

- installs Grafana OSS 13.2.3 from its release `.deb` (about 380 MB, checked against a pinned sha256, as Prometheus is) with its apt dependencies;
- writes [`grafana.ini`](grafana.ini) (loopback `127.0.0.1:3002`, no sign-up, no anonymous access, no telemetry, logs to the journal), the data source ([`datasources.yaml`](datasources.yaml): Prometheus at `127.0.0.1:9090`) and the dashboard provider ([`dashboards.yaml`](dashboards.yaml)), and copies the two dashboards to `/etc/grafana/dashboards/stellar-explorer/`;
- sets the admin password to `codemelt` (`grafanaAdminPassword` in `cli/grafana.go`). Grafana reads it from `/etc/grafana/admin-password` (root:grafana 0640) when it creates its database; after that, a run that finds it changed sets it back with `grafana cli admin reset-admin-password`;
- enables and starts `grafana-server`, and restarts it when its configuration or version changed (Grafana has no reload; a restart costs nothing here).

With the clickhouse host on, `lab grafana` adds ClickHouse too ([below](#clickhouse)).

Once Grafana is installed, every `lab psql` run keeps it in the documented state too (a restart there still needs `--allow-restart`), and `lab psql check` reports it. Before that, `lab psql` leaves it out.

## ClickHouse

When the psql host's `clickhouse` step is on (`clickhouse_hostname` set and the clickhouse bundle installed, [README.md §10](../README.md)), the `grafana` step, which runs after it:

- installs the ClickHouse data source plugin `grafana-clickhouse-datasource` 4.22.1 from grafana.com (checked against a pinned sha256) into `/var/lib/grafana/plugins/`;
- writes the data source ([`clickhouse-datasource.yaml`](clickhouse-datasource.yaml), uid `clickhouse`): ClickHouse's native protocol over TLS at `10.0.2.2:9440`, trusting only lab's CA for it (`/etc/stellar/clickhouse-ca.crt`), as `stellar_reader` with the bundle's password (copied to `/etc/grafana/clickhouse-reader.password`, root:grafana 0640), database `stellar`;
- copies [`clickhouse.json`](clickhouse.json) to the dashboards folder;
- restarts Grafana when the data source, its password or the plugin changed.

Viewer accounts can't open *Explore* (it takes the Editor role), but they can send any query through a data source with Grafana's query API, which the dashboards use. So ClickHouse's `reader` profile and quota bound what they can do: read only, 30 s and 4 GB a query ([clickhouse/README.md §5.3](../clickhouse/README.md#53-users)). They have their own budget, apart from the public SQL endpoint's `stellar_public`.

`lab grafana check` asks the ClickHouse data source's health too. With `clickhouse_hostname = "none"`, `lab grafana` removes the data source, its password and the dashboard (the plugin stays, unused).

## Open it

Grafana listens on loopback only, like Prometheus and Alertmanager. Open it through an SSH tunnel, or give it a public hostname.

**Through a tunnel:**

```sh
ssh -L 3002:127.0.0.1:3002 root@<psql host public IP>
# then http://localhost:3002, user admin, password codemelt
```

**Publicly**, at `https://<grafana_hostname>`:

1. Point a DNS name at the psql host, for example `grafana.api.example.com`.
2. Set `grafana_hostname` to it in `/etc/stellar/lab-psql.toml`.
3. Run `/root/bin/lab grafana`.

The `grafana` step then:
- adds a Caddy site for that name ([`psql/files/grafana.caddy`](../psql/files/grafana.caddy), in `/etc/caddy/sites/`). Caddy gets its certificate and proxies to Grafana on loopback;
- gives Grafana its public URL ([`grafana-public.conf`](grafana-public.conf), a drop-in for `grafana-server.service`).

What protects it:
- Grafana's login: user `admin` with the generated password, no sign-up, no anonymous access, and Grafana's lockout after repeated failed logins;
- Caddy's limit of 10 login attempts a minute per client IP.

**Accounts for viewing.** Give people who only look at the dashboards a Viewer account rather than the admin's password. A Viewer sees every dashboard but can't edit them or add data sources, which on this host could reach loopback services such as Caddy's admin API. Accounts live in Grafana's database (`/var/lib/grafana`), not in `lab`'s files, so a reinstall from scratch needs them added again:

```sh
curl -u admin:codemelt -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:3002/api/admin/users --data '{"name":"<name>","login":"<login>","password":"<password>"}'
# new users get the Viewer role (Grafana's default); check with GET /api/org/users
```

The tunnel keeps working (`csrf_trusted_origins` in `grafana.ini`). To take it offline again, set `grafana_hostname = "none"` and run `/root/bin/lab grafana`: the step removes both files.

The dashboards are in the folder *Stellar explorer*; the pipeline dashboard is the home page.

## Change it

- **A dashboard:** edit its JSON here (or build it in the UI, then *Export → JSON* and replace the file), `make dist`, copy `bin/` over, `lab grafana`. Grafana re-reads the folder every 30 s, without a restart. The provisioned dashboards can't be saved from the UI (`allowUiUpdates: false`), so the repository stays the source.
- **A new dashboard:** add a `*.json` file here; it is embedded and installed like the others. Give it a unique `uid`, the data source `{"type": "prometheus", "uid": "prometheus"}`, and the tag `stellar-explorer`. Only `clickhouse.json` also uses `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}`, since it comes and goes with the clickhouse host (`cli/cli_test.go` checks both).
- **A ClickHouse panel:** test its SQL as `stellar_reader` first. A column alias is visible in the whole query, so an alias with a column's name (`countIf(successful) AS successful`) changes what the rest of the query reads; give aliases their own names.
- **The configuration** (`grafana.ini`, the two YAML files): edit, rebuild, `lab grafana`; it restarts Grafana.
- **The admin password:** it is fixed, `grafanaAdminPassword` in `cli/grafana.go`. Changed from the UI, the next `lab psql` or `lab grafana` sets it back. To change it for good, change the constant, rebuild and run `lab grafana`. Finding it changed costs lab one failed login, and Grafana blocks a user's logins after 5 failures in a row, until 5 minutes after the last. So a run tries the old password at most once and never retries; if a run says Grafana refuses the password it just set, wait the 5 minutes and run it again.
- **Upgrading Grafana:** change the version and sha256 in `cli/grafana.go` (`grafanaVersion`, `grafanaSHA256`; the sha256 is published next to the package as `grafana_<version>_amd64.deb.sha256` on dl.grafana.com), rebuild, `lab grafana`.
- **Upgrading the ClickHouse plugin:** change `chPluginVersion` and `chPluginSHA256` in `cli/grafana_clickhouse.go` (grafana.com publishes the sha256 per platform: `GET https://grafana.com/api/plugins/grafana-clickhouse-datasource/versions/<version>`, `packages."linux-amd64".sha256`), check the version's minimum Grafana there, rebuild, `lab grafana`.

The queries use only metric names that the worker, the exporters, PostgREST and Caddy export; the alert rules in `galexie/deploy/alerts.yml` and `psql/files/alerts.yml` use the same ones. A query that shows *No data* usually means its target is down: look at the *Scrape targets up* panel, or at `http://localhost:9090/targets` through the tunnel.

## Files

| File | Installed as |
|---|---|
| `grafana.ini` | `/etc/grafana/grafana.ini` (root:grafana 0640) |
| `grafana-public.conf` | `/etc/systemd/system/grafana-server.service.d/20-public.conf`, only while `grafana_hostname` is set (its Caddy site is `psql/files/grafana.caddy`) |
| `datasources.yaml` | `/etc/grafana/provisioning/datasources/stellar-explorer.yaml` |
| `dashboards.yaml` | `/etc/grafana/provisioning/dashboards/stellar-explorer.yaml` |
| `pipeline.json`, `psql.json` | `/etc/grafana/dashboards/stellar-explorer/` |
| `clickhouse-datasource.yaml` | `/etc/grafana/provisioning/datasources/stellar-clickhouse.yaml` (root:grafana 0640), with the clickhouse host |
| `clickhouse.json` | `/etc/grafana/dashboards/stellar-explorer/`, with the clickhouse host |
| `files.go` | (not installed) embeds the files above for `lab` |
