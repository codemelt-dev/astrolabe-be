// Package config loads /etc/stellar/worker.toml and the credential env files.
//
// The credential files (gateway.env, archive.env) are parsed by path and are
// never loaded into the process environment, so no worker process carries
// AWS_* variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// DefaultPath is where every subcommand looks for its config by default.
const DefaultPath = "/etc/stellar/worker.toml"

// Config is the worker's configuration file. Unknown keys are rejected so a
// typo can never silently fall back to a default.
type Config struct {
	// LogLevel is one of debug, info, warn, error.
	LogLevel    string      `toml:"log_level"`
	Credentials Credentials `toml:"credentials"`
	Lake        Lake        `toml:"lake"`
	Archive     Archive     `toml:"archive"`
	Postgres    Postgres    `toml:"postgres"`
	Ingest      Ingest      `toml:"ingest"`
	Bootstrap   Bootstrap   `toml:"bootstrap"`
	Prune       Prune       `toml:"prune"`
	Metrics     Metrics     `toml:"metrics"`
	Galexie     Galexie     `toml:"galexie"`
	Watchdog    Watchdog    `toml:"watchdog"`
	Gapcheck    Gapcheck    `toml:"gapcheck"`
	ClickHouse  ClickHouse  `toml:"clickhouse"`
}

// Credentials points at the env files holding secrets.
type Credentials struct {
	// GatewayEnv holds the gateway root keys, shared with Galexie's container.
	GatewayEnv string `toml:"gateway_env"`
	// ArchiveEnv holds the archive endpoint and uploader key.
	ArchiveEnv string `toml:"archive_env"`
}

// Lake configures `worker lake` and how other subcommands reach it.
type Lake struct {
	// Dir is the lake Volume mountpoint. The gateway root is Dir/s3 and the
	// lake's own state (lake-id marker, boot-id, bootstrap.json) is Dir/state.
	Dir string `toml:"dir"`
	// BucketPath is "<bucket>/<prefix>" and must equal Galexie's
	// destination_bucket_path.
	BucketPath string `toml:"bucket_path"`
	// Region must equal Galexie's region, or SigV4 requests fail with 400.
	Region string `toml:"region"`
	// S3Listen is the gateway address (loopback only).
	S3Listen string `toml:"s3_listen"`
	// ControlListen serves /v1/tip, /metrics and /healthz (loopback only).
	ControlListen string `toml:"control_listen"`
	// MetricsListen optionally serves /metrics and /healthz on the private
	// interface ("<private IP>:9101"). It is bound best-effort. Empty
	// disables it.
	MetricsListen string `toml:"metrics_listen"`
	// AllowUnmounted skips the mountpoint check (the lake-id marker is still
	// required). For development and tests only.
	AllowUnmounted bool `toml:"allow_unmounted"`
}

// Archive configures the archive bucket, a 1:1 copy of the lake kept
// forever: same keys, bytes, metadata and manifest, so its key prefix is
// always the lake's. The endpoint, region and uploader key come from
// credentials.archive_env.
type Archive struct {
	// Enabled runs the archiver. While it is on, dev-feed refuses to run
	// unless the bucket name ends in "-dev", so ledgers copied from the AWS
	// lake can never reach a real archive.
	Enabled bool `toml:"enabled"`
	// Bucket is the archive bucket name.
	Bucket string `toml:"bucket"`
	// PathStyle selects path-style addressing; the default is
	// virtual-hosted.
	PathStyle bool `toml:"path_style"`
}

// BucketPath returns "<bucket>/<lake prefix>", the archive's
// destination_bucket_path.
func (a Archive) BucketPath(lake Lake) string {
	if p := lake.Prefix(); p != "" {
		return a.Bucket + "/" + p
	}
	return a.Bucket
}

// bucketNameRe is S3's bucket naming rule (3-63 characters; lower-case
// letters, digits, dots and hyphens; starting and ending alphanumeric).
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func (a Archive) validate() error {
	if !bucketNameRe.MatchString(a.Bucket) {
		return fmt.Errorf("archive.bucket %q is not a valid S3 bucket name", a.Bucket)
	}
	return nil
}

// Postgres locates the explorer database. Keep passwords
// out of this file: libpq's passfile=<path> parameter (a pgpass file) works
// in both DSNs.
type Postgres struct {
	// DSN is used by ingest, bootstrap and reindex (role ingest_writer).
	// Commands that need Postgres refuse to run while it is empty.
	DSN string `toml:"dsn"`
	// MigrateDSN is used only by `worker migrate` (role explorer_owner, which
	// owns the schema). Empty means DSN.
	MigrateDSN string `toml:"migrate_dsn"`
}

// MigrationDSN returns the DSN `worker migrate` connects with.
func (p Postgres) MigrationDSN() string {
	if p.MigrateDSN != "" {
		return p.MigrateDSN
	}
	return p.DSN
}

// ClickHouse configures the clickhouse pipeline (`worker clickhouse …`, on
// the clickhouse host; clickhouse/README.md) and `worker state-copy` (on the
// psql host). Passwords are read from files, never kept here.
type ClickHouse struct {
	// Addr is ClickHouse's native protocol address: loopback on the
	// clickhouse host; its private IP and TLS port (9440) elsewhere.
	Addr     string `toml:"addr"`
	Database string `toml:"database"`
	// User logs in for ingest, init and check (stellar_writer), or
	// state-copy (stellar_jobs).
	User         string `toml:"user"`
	PasswordFile string `toml:"password_file"`
	// CAFile, when set, makes the connection TLS, trusting only this CA
	// (lab's clickhouse CA).
	CAFile string `toml:"ca_file"`
	// MigrateUser logs in for `worker clickhouse-migrate` only (stellar_owner).
	MigrateUser         string `toml:"migrate_user"`
	MigratePasswordFile string `toml:"migrate_password_file"`
	// TipPoll is how often the archive's newest ledger is listed.
	TipPoll time.Duration `toml:"tip_poll"`
}

var clickhouseNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func (c ClickHouse) validate() error {
	var errs []error
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		errs = append(errs, fmt.Errorf("clickhouse.addr %q: want host:port", c.Addr))
	}
	for name, v := range map[string]string{"clickhouse.database": c.Database, "clickhouse.user": c.User, "clickhouse.migrate_user": c.MigrateUser} {
		if !clickhouseNameRe.MatchString(v) {
			errs = append(errs, fmt.Errorf("%s %q: want lower-case letters, digits and _", name, v))
		}
	}
	errs = append(errs, absPath("clickhouse.password_file", c.PasswordFile), absPath("clickhouse.migrate_password_file", c.MigratePasswordFile))
	if c.CAFile != "" {
		errs = append(errs, absPath("clickhouse.ca_file", c.CAFile))
	}
	if c.TipPoll < time.Second {
		errs = append(errs, fmt.Errorf("clickhouse.tip_poll %s: want at least 1s", c.TipPoll))
	}
	return errors.Join(errs...)
}

// Ingest tunes the sequencer of `worker ingest`.
type Ingest struct {
	// LakeGrace is how long the lake may be unavailable before the sequencer
	// fails over to the archive or the AWS lake (and alerts). Shorter
	// outages, such as a lake restart, are ridden out with retries.
	LakeGrace time.Duration `toml:"lake_grace"`
	// ArchiveGrace is how long reads from the archive may keep failing
	// before the sequencer falls back to the AWS lake (and alerts).
	ArchiveGrace time.Duration `toml:"archive_grace"`
	// MaxBatchLedgers and MaxBatchMiB bound one catch-up commit (at most
	// 500 ledgers and 512 MiB of prepared rows).
	MaxBatchLedgers int `toml:"max_batch_ledgers"`
	MaxBatchMiB     int `toml:"max_batch_mib"`
}

// The largest batch limits allowed.
const (
	MaxBatchLedgersLimit = 500
	MaxBatchMiBLimit     = 512
)

func (i Ingest) validate() error {
	var errs []error
	if i.LakeGrace < time.Second {
		errs = append(errs, fmt.Errorf("ingest.lake_grace %s: want at least 1s", i.LakeGrace))
	}
	if i.ArchiveGrace < time.Second {
		errs = append(errs, fmt.Errorf("ingest.archive_grace %s: want at least 1s", i.ArchiveGrace))
	}
	if i.MaxBatchLedgers < 1 || i.MaxBatchLedgers > MaxBatchLedgersLimit {
		errs = append(errs, fmt.Errorf("ingest.max_batch_ledgers %d: want 1-%d", i.MaxBatchLedgers, MaxBatchLedgersLimit))
	}
	if i.MaxBatchMiB < 1 || i.MaxBatchMiB > MaxBatchMiBLimit {
		errs = append(errs, fmt.Errorf("ingest.max_batch_mib %d: want 1-%d", i.MaxBatchMiB, MaxBatchMiBLimit))
	}
	return errors.Join(errs...)
}

// Default returns the production defaults.
func Default() Config {
	c := Config{
		LogLevel: "info",
		Credentials: Credentials{
			GatewayEnv: "/etc/stellar/gateway.env",
			ArchiveEnv: "/etc/stellar/archive.env",
		},
		Lake: Lake{
			Dir:           "/srv/lake",
			BucketPath:    "stellar/ledgers/pubnet",
			Region:        "us-east-1",
			S3Listen:      "127.0.0.1:9000",
			ControlListen: "127.0.0.1:9001",
		},
		Archive: Archive{
			Enabled: true,
			Bucket:  "stellar-ledgers-pubnet",
		},
		Ingest: Ingest{
			LakeGrace:       2 * time.Minute,
			ArchiveGrace:    10 * time.Minute,
			MaxBatchLedgers: MaxBatchLedgersLimit,
			MaxBatchMiB:     MaxBatchMiBLimit,
		},
	}
	c.ClickHouse = ClickHouse{
		Addr:                "127.0.0.1:9000",
		Database:            "stellar",
		User:                "stellar_writer",
		PasswordFile:        "/etc/stellar/clickhouse-writer.password",
		MigrateUser:         "stellar_owner",
		MigratePasswordFile: "/etc/stellar/clickhouse-owner.password",
		TipPoll:             5 * time.Second,
	}
	defaultOperations(&c)
	return c
}

// Load reads the file at path over the defaults and validates the result.
func Load(path string) (Config, error) {
	cfg := Default()
	md, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, len(undecoded))
		for i, k := range undecoded {
			keys[i] = k.String()
		}
		sort.Strings(keys)
		return Config{}, fmt.Errorf("config %s: unknown keys: %s", path, strings.Join(keys, ", "))
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks every field. All paths must be absolute, because the
// embedded gateway changes the process working directory.
func (c Config) Validate() error {
	var errs []error
	if _, err := c.SlogLevel(); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs,
		absPath("credentials.gateway_env", c.Credentials.GatewayEnv),
		absPath("credentials.archive_env", c.Credentials.ArchiveEnv),
		c.Lake.validate(),
		c.Archive.validate(),
		c.Ingest.validate(),
		c.ClickHouse.validate(),
	)
	errs = append(errs, c.validateOperations()...)
	return errors.Join(errs...)
}

// SlogLevel returns LogLevel as a slog level.
func (c Config) SlogLevel() (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(c.LogLevel)); err != nil {
		return 0, fmt.Errorf("log_level %q: want debug, info, warn or error", c.LogLevel)
	}
	return l, nil
}

func (l Lake) validate() error {
	var errs []error
	errs = append(errs, absPath("lake.dir", l.Dir))
	if l.Dir == "/" {
		errs = append(errs, errors.New("lake.dir must not be /"))
	}
	if _, _, err := SplitBucketPath(l.BucketPath); err != nil {
		errs = append(errs, fmt.Errorf("lake.bucket_path: %w", err))
	}
	if l.Region == "" {
		errs = append(errs, errors.New("lake.region is required"))
	}
	errs = append(errs,
		loopbackAddr("lake.s3_listen", l.S3Listen),
		loopbackAddr("lake.control_listen", l.ControlListen),
	)
	if l.MetricsListen != "" {
		if _, err := netip.ParseAddrPort(l.MetricsListen); err != nil {
			errs = append(errs, fmt.Errorf("lake.metrics_listen %q: want <ip>:<port>: %w", l.MetricsListen, err))
		}
	}
	return errors.Join(errs...)
}

// Bucket returns the bucket part of BucketPath.
func (l Lake) Bucket() string { b, _, _ := SplitBucketPath(l.BucketPath); return b }

// Prefix returns the key prefix part of BucketPath ("" for the bucket root).
func (l Lake) Prefix() string { _, p, _ := SplitBucketPath(l.BucketPath); return p }

// SplitBucketPath splits "<bucket>/<prefix>" like Galexie's
// destination_bucket_path. The prefix may be empty.
func SplitBucketPath(bp string) (bucket, prefix string, err error) {
	if bp == "" {
		return "", "", errors.New("empty")
	}
	if strings.HasPrefix(bp, "/") || strings.HasSuffix(bp, "/") || path.Clean(bp) != bp {
		return "", "", fmt.Errorf("%q is not a clean <bucket>/<prefix> path", bp)
	}
	bucket, prefix, _ = strings.Cut(bp, "/")
	if bucket == "." || bucket == ".." {
		return "", "", fmt.Errorf("%q is not a clean <bucket>/<prefix> path", bp)
	}
	return bucket, prefix, nil
}

func absPath(name, p string) error {
	if p == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("%s %q must be a clean absolute path", name, p)
	}
	return nil
}

func loopbackAddr(name, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q: %w", name, addr, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsLoopback() {
		return fmt.Errorf("%s %q must be a loopback <ip>:<port>", name, addr)
	}
	return nil
}

// AWSEnvVars lists AWS_* variables present in the process environment. The
// worker never needs them; their presence means the unit is misconfigured.
func AWSEnvVars() []string {
	var names []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "AWS_") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
