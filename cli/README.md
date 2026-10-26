# cli: `lab`, the setup CLI

`lab` sets up the data layer's servers and checks them. Each host has a step table; every step checks the host first and changes only what is not in the documented state, so running `lab` again on a configured host changes nothing. `lab <host> check` only reports.

| Command | Host | Documentation |
|---|---|---|
| `lab galexie` | galexie host | [`galexie/README.md`](../galexie/README.md#setup-with-lab-galexie) |
| `lab psql` | psql host | [`psql/README.md`](../psql/README.md#setup-with-lab-psql) |
| `lab clickhouse` | clickhouse host | [`clickhouse/README.md`](../clickhouse/README.md#setup-with-lab-clickhouse) |
| `lab grafana` | psql host | [`dashboard/README.md`](../dashboard/README.md) |

`lab` runs only as root on Ubuntu 24.04 amd64. Flags: `--yes`, `--allow-restart`, `--only`, `--skip`, `--config`; `lab <host> --help` lists the host's steps.

## How it works

- **Step engine** (`run.go`): check every step, list what will change, ask once, apply in order, check each step again. Output is one line per step (`✓`, `↻`, `◷`, `!`, `✗`); every command and its output go to `/var/log/stellar-setup/<host>.log`, and a lock prevents concurrent runs.
- **Settings and secrets** (`config.go`): missing settings are asked in one form and saved to `/etc/stellar/lab-<host>.toml`; secrets are entered in password fields or generated, and kept in `/etc/stellar/secrets/` (root only), never written to the settings file, the terminal or the log.
- **Templates:** the files each host installs are embedded from the host folders (`galexie/deploy/`, `psql/files/`, `clickhouse/files/`, `dashboard/`) through their `files.go`. They are committed with example values, and `lab` replaces each example value with the configured one (whole tokens only) at install time.
- **Never overwritten:** a file in the way that `lab` does not recognise (no `lab` header and not a package default) stops its step instead of being replaced.
- **Restarts:** configuration changes that a reload picks up are reloaded; running services are restarted only with `--allow-restart`.
- **Host helpers** (`sys.go`): commands, atomic files, directories, apt, users, systemd units, verified downloads (every downloaded release is checked against a pinned sha256).

## Files

| File | Purpose |
|---|---|
| `main.go` | the command line (cobra with fang) |
| `run.go` | the step engine, output, health probes, lock and log |
| `sys.go` | host helpers |
| `config.go` | settings, secrets, template rendering |
| `galexie.go`, `galexie_host.go`, `galexie_pipeline.go` | galexie host steps |
| `psql.go`, `psql_pg.go`, `psql_web.go`, `psql_clickhouse.go`, `psql_jobs.go` | psql host steps |
| `grafana.go`, `grafana_clickhouse.go` | Grafana |
| `clickhouse.go` | clickhouse host steps |
| `cli_test.go` | rendering with the example values changes no file and uses every example; tuning calculations; SCRAM verifiers; step order |

## Building and testing

At the repository root:

```sh
make build            # bin/lab-<os>-<arch> for this machine (--help only)
make test             # go test
make check            # gofmt, vet, tests, Linux builds of lab and Caddy
make dist VERSION=…   # bin/lab, bin/worker, bin/caddy for linux/amd64
```

`lab` cannot run on macOS (no systemd or apt). Its end-to-end test is a fresh Ubuntu 24.04 server per host: run it twice (the second run must change nothing), then `check`, a reboot, and `check` again.
