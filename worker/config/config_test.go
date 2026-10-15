package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, name, content string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeFile(t, "worker.toml", "", 0o600))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("empty file should yield defaults, got %+v", cfg)
	}
	if cfg.Lake.Bucket() != "stellar" || cfg.Lake.Prefix() != "ledgers/pubnet" {
		t.Fatalf("bucket/prefix = %q/%q", cfg.Lake.Bucket(), cfg.Lake.Prefix())
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(writeFile(t, "worker.toml", `
log_level = "debug"
[credentials]
gateway_env = "/x/gateway.env"
[lake]
dir = "/data/lake"
bucket_path = "b"
metrics_listen = "10.0.0.2:9101"
allow_unmounted = true
`, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Lake.Dir != "/data/lake" || cfg.Lake.Bucket() != "b" || cfg.Lake.Prefix() != "" || !cfg.Lake.AllowUnmounted ||
		cfg.Credentials.GatewayEnv != "/x/gateway.env" || cfg.Credentials.ArchiveEnv != Default().Credentials.ArchiveEnv {
		t.Fatalf("unexpected %+v", cfg)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]string{
		"unknown key":       "[lake]\ndirr = \"/srv/lake\"\n",
		"unknown section":   "[laek]\ndir = \"/srv/lake\"\n",
		"relative dir":      "[lake]\ndir = \"srv/lake\"\n",
		"root dir":          "[lake]\ndir = \"/\"\n",
		"unclean dir":       "[lake]\ndir = \"/srv/lake/\"\n",
		"relative env":      "[credentials]\ngateway_env = \"gateway.env\"\n",
		"public s3 listen":  "[lake]\ns3_listen = \"0.0.0.0:9000\"\n",
		"hostname listen":   "[lake]\ncontrol_listen = \"localhost:9001\"\n",
		"bad metrics addr":  "[lake]\nmetrics_listen = \"eth1:9101\"\n",
		"bad bucket path":   "[lake]\nbucket_path = \"/stellar/x\"\n",
		"dotdot bucket":     "[lake]\nbucket_path = \"stellar/../x\"\n",
		"bad log level":     "log_level = \"loud\"\n",
		"empty region":      "[lake]\nregion = \"\"\n",
		"toml syntax error": "[lake\n",
		"unknown pg key":    "[postgres]\nurl = \"postgres://x\"\n",
		"short lake grace":  "[ingest]\nlake_grace = \"10ms\"\n",
		"bad grace":         "[ingest]\narchive_grace = \"soon\"\n",
		"batch too large":   "[ingest]\nmax_batch_ledgers = 501\n",
		"batch too small":   "[ingest]\nmax_batch_ledgers = 0\n",
		"batch MiB too big": "[ingest]\nmax_batch_mib = 1024\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeFile(t, "worker.toml", content, 0o600)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.toml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestGatewayEnv(t *testing.T) {
	p := writeFile(t, "gateway.env", "# comment\n\nAWS_ACCESS_KEY_ID=AKID\nAWS_SECRET_ACCESS_KEY=s3cr=et/+\nAWS_EC2_METADATA_DISABLED=true\n", 0o640)
	c, err := LoadGatewayEnv(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.AccessKey != "AKID" || c.SecretKey != "s3cr=et/+" {
		t.Fatalf("got %+v", c)
	}
	if s := c.String(); strings.Contains(s, "s3cr") {
		t.Fatalf("String leaks the secret: %s", s)
	}
}

func TestGatewayEnvRejects(t *testing.T) {
	cases := map[string]struct {
		content string
		mode    os.FileMode
	}{
		"quoted":          {"AWS_ACCESS_KEY_ID=\"a\"\nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"single quoted":   {"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY='b'\n", 0o640},
		"trailing space":  {"AWS_ACCESS_KEY_ID=a \nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"missing secret":  {"AWS_ACCESS_KEY_ID=a\n", 0o640},
		"unknown key":     {"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\nAWS_REGION=x\n", 0o640},
		"duplicate":       {"AWS_ACCESS_KEY_ID=a\nAWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"export prefix":   {"export AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"no equals":       {"AWS_ACCESS_KEY_ID\nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"empty value":     {"AWS_ACCESS_KEY_ID=\nAWS_SECRET_ACCESS_KEY=b\n", 0o640},
		"metadata false":  {"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\nAWS_EC2_METADATA_DISABLED=false\n", 0o640},
		"world readable":  {"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n", 0o644},
		"world writeable": {"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n", 0o642},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p := writeFile(t, "gateway.env", tc.content, 0o600)
			if err := os.Chmod(p, tc.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGatewayEnv(p); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestArchiveEnv(t *testing.T) {
	p := writeFile(t, "archive.env", "ARCHIVE_S3_ENDPOINT=https://hel1.your-objectstorage.com\nARCHIVE_S3_REGION=hel1\nARCHIVE_S3_ACCESS_KEY=ak\nARCHIVE_S3_SECRET_KEY=sk\n", 0o640)
	c, err := LoadArchiveEnv(p)
	if err != nil {
		t.Fatal(err)
	}
	want := ArchiveCredentials{Endpoint: "https://hel1.your-objectstorage.com", Region: "hel1", AccessKey: "ak", SecretKey: "sk"}
	if c != want {
		t.Fatalf("got %+v", c)
	}
	for name, content := range map[string]string{
		"aws names":     "AWS_ACCESS_KEY_ID=ak\nARCHIVE_S3_ENDPOINT=https://x\nARCHIVE_S3_REGION=r\nARCHIVE_S3_ACCESS_KEY=ak\nARCHIVE_S3_SECRET_KEY=sk\n",
		"bad endpoint":  "ARCHIVE_S3_ENDPOINT=hel1.your-objectstorage.com\nARCHIVE_S3_REGION=r\nARCHIVE_S3_ACCESS_KEY=ak\nARCHIVE_S3_SECRET_KEY=sk\n",
		"endpoint path": "ARCHIVE_S3_ENDPOINT=https://x/bucket\nARCHIVE_S3_REGION=r\nARCHIVE_S3_ACCESS_KEY=ak\nARCHIVE_S3_SECRET_KEY=sk\n",
		"missing":       "ARCHIVE_S3_ENDPOINT=https://x\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadArchiveEnv(writeFile(t, "archive.env", content, 0o640)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestSplitBucketPath(t *testing.T) {
	for in, want := range map[string][2]string{
		"stellar/ledgers/pubnet": {"stellar", "ledgers/pubnet"},
		"stellar":                {"stellar", ""},
	} {
		b, p, err := SplitBucketPath(in)
		if err != nil || b != want[0] || p != want[1] {
			t.Errorf("SplitBucketPath(%q) = %q, %q, %v", in, b, p, err)
		}
	}
	for _, in := range []string{"", "/a", "a/", "a//b", "./a", "..", "a/../b"} {
		if _, _, err := SplitBucketPath(in); err == nil {
			t.Errorf("SplitBucketPath(%q) accepted", in)
		}
	}
}

func TestArchiveConfig(t *testing.T) {
	d := Default()
	if !d.Archive.Enabled || d.Archive.Bucket != "stellar-ledgers-pubnet" || d.Archive.PathStyle {
		t.Fatalf("archive defaults %+v", d.Archive)
	}
	// Same keys as the lake: the archive's prefix is always the lake's.
	if got := d.Archive.BucketPath(d.Lake); got != "stellar-ledgers-pubnet/ledgers/pubnet" {
		t.Fatalf("BucketPath = %q", got)
	}
	if got := d.Archive.BucketPath(Lake{BucketPath: "b"}); got != "stellar-ledgers-pubnet" {
		t.Fatalf("BucketPath at a bucket root = %q", got)
	}
	cfg, err := Load(writeFile(t, "worker.toml", "[archive]\nenabled = false\nbucket = \"stellar-ledgers-pubnet-dev\"\npath_style = true\n", 0o600))
	if err != nil || cfg.Archive != (Archive{Enabled: false, Bucket: "stellar-ledgers-pubnet-dev", PathStyle: true}) {
		t.Fatalf("Load = %+v, %v", cfg.Archive, err)
	}
	for _, bad := range []string{"", "ab", "Upper-Case", "has/slash", "-leading-dash", "trailing-dash-", "under_score"} {
		if _, err := Load(writeFile(t, "worker.toml", "[archive]\nbucket = \""+bad+"\"\n", 0o600)); err == nil {
			t.Errorf("bucket %q accepted", bad)
		}
	}
	if _, err := Load(writeFile(t, "worker.toml", "[archive]\nbucket_path = \"x/y\"\n", 0o600)); err == nil {
		t.Error("unknown archive key accepted")
	}
}

func TestPostgresAndIngest(t *testing.T) {
	d := Default()
	if d.Postgres.DSN != "" || d.Ingest.LakeGrace != 2*time.Minute || d.Ingest.ArchiveGrace != 10*time.Minute ||
		d.Ingest.MaxBatchLedgers != 500 || d.Ingest.MaxBatchMiB != 512 {
		t.Fatalf("defaults: %+v %+v", d.Postgres, d.Ingest)
	}
	cfg, err := Load(writeFile(t, "worker.toml", `
[postgres]
dsn = "postgres://ingest_writer@10.0.1.2/explorer?sslmode=verify-full"
[ingest]
lake_grace = "30s"
archive_grace = "5m"
max_batch_ledgers = 100
max_batch_mib = 64
`, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.MigrationDSN() != cfg.Postgres.DSN || cfg.Ingest.LakeGrace != 30*time.Second ||
		cfg.Ingest.ArchiveGrace != 5*time.Minute || cfg.Ingest.MaxBatchLedgers != 100 || cfg.Ingest.MaxBatchMiB != 64 {
		t.Fatalf("unexpected %+v %+v", cfg.Postgres, cfg.Ingest)
	}
	cfg.Postgres.MigrateDSN = "postgres://explorer_owner@10.0.1.2/explorer"
	if cfg.Postgres.MigrationDSN() != cfg.Postgres.MigrateDSN {
		t.Fatal("migrate_dsn not preferred")
	}
}

// The psql host's state-copy reaches ClickHouse over TLS: a CA file makes
// the connection TLS, and must be an absolute path.
func TestClickHouseCAFile(t *testing.T) {
	if Default().ClickHouse.CAFile != "" {
		t.Fatal("TLS by default")
	}
	cfg, err := Load(writeFile(t, "worker.toml", `
[postgres]
dsn = "host=/var/run/postgresql dbname=explorer user=state_reader"
[clickhouse]
addr = "10.0.2.2:9440"
user = "stellar_jobs"
password_file = "/etc/stellar/clickhouse-jobs.password"
ca_file = "/etc/stellar/clickhouse-ca.crt"
`, 0o600))
	if err != nil || cfg.ClickHouse.CAFile != "/etc/stellar/clickhouse-ca.crt" || cfg.ClickHouse.User != "stellar_jobs" {
		t.Fatalf("%+v %v", cfg.ClickHouse, err)
	}
	if _, err := Load(writeFile(t, "worker.toml", "[clickhouse]\nca_file = \"ca.crt\"\n", 0o600)); err == nil || !strings.Contains(err.Error(), "clickhouse.ca_file") {
		t.Fatalf("relative ca_file: %v", err)
	}
}
