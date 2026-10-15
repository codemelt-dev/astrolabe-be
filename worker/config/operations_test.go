package config

import (
	"strings"
	"testing"
	"time"
)

func TestOperationsDefaults(t *testing.T) {
	d := Default()
	if d.Prune.RetentionLedgers != 51_840 || !d.Prune.Enabled || d.Prune.LocalReaders[0] != "explorer" ||
		d.Metrics.Listen != "127.0.0.1:9102" || d.Watchdog.ReceivingRestart != 120*time.Second ||
		d.Watchdog.UploaderStall != 180*time.Second || d.Watchdog.SyncingPage != 30*time.Second ||
		d.Watchdog.MinRestartInterval != 15*time.Minute || d.Watchdog.PageAfterRestarts != 2 ||
		d.Gapcheck.Method != GapcheckDetectGaps || len(d.Bootstrap.HistoryArchives) != 3 {
		t.Fatalf("defaults %+v", d)
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	// Default() hands out its own slices.
	d.Bootstrap.HistoryArchives[0] = "x"
	if Default().Bootstrap.HistoryArchives[0] == "x" || PubnetHistoryArchives[0] == "x" {
		t.Fatal("defaults share a slice")
	}
}

func TestOperationsOverrides(t *testing.T) {
	cfg, err := Load(writeFile(t, "worker.toml", `
[bootstrap]
history_archives = ["file:///tmp/archive"]
checkpoint_poll = "5s"
[prune]
retention_ledgers = 17280
disk_high_watermark = 0.9
local_readers = ["explorer", "second"]
[metrics]
listen = "127.0.0.1:19102"
private_listen = "10.0.0.2:9102"
ready_max_lag = 10
[galexie]
image = "stellar/stellar-galexie:29.0.0@sha256:abc"
[watchdog]
state_file = "/tmp/wd.json"
first_upload_within = "1h"
[gapcheck]
method = "reindex-dry-run"
`, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bootstrap.CheckpointPoll != 5*time.Second || cfg.Prune.RetentionLedgers != 17_280 || len(cfg.Prune.LocalReaders) != 2 ||
		cfg.Metrics.PrivateListen != "10.0.0.2:9102" || cfg.Watchdog.FirstUploadWithin != time.Hour ||
		cfg.Gapcheck.Method != GapcheckDryRun || cfg.Galexie.Image == "" {
		t.Fatalf("%+v", cfg)
	}
}

func TestOperationsRejects(t *testing.T) {
	for name, tc := range map[string]struct{ toml, want string }{
		"retention below 24 h":    {"[prune]\nretention_ledgers = 100\n", "prune.retention_ledgers"},
		"retention above 72 h":    {"[prune]\nretention_ledgers = 60000\n", "prune.retention_ledgers"},
		"watermark":               {"[prune]\ndisk_high_watermark = 0.99\n", "prune.disk_high_watermark"},
		"reindex reader":          {"[prune]\nlocal_readers = [\"reindex:1-2\"]\n", "prune.local_readers"},
		"public metrics":          {"[metrics]\nlisten = \"0.0.0.0:9102\"\n", "metrics.listen"},
		"bad private":             {"[metrics]\nprivate_listen = \"eth0\"\n", "metrics.private_listen"},
		"relative core dir":       {"[metrics]\ncore_dir = \"srv/core\"\n", "metrics.core_dir"},
		"no archives":             {"[bootstrap]\nhistory_archives = []\n", "bootstrap.history_archives"},
		"ftp archive":             {"[bootstrap]\nhistory_archives = [\"ftp://x\"]\n", "bootstrap.history_archives"},
		"galexie metrics url":     {"[galexie]\nmetrics_url = \"6061\"\n", "galexie.metrics_url"},
		"relative state file":     {"[watchdog]\nstate_file = \"state.json\"\n", "watchdog.state_file"},
		"zero restart interval":   {"[watchdog]\nmin_restart_interval = \"0s\"\n", "watchdog.min_restart_interval"},
		"no pages":                {"[watchdog]\npage_after_restarts = 0\n", "watchdog.page_after_restarts"},
		"unknown gapcheck":        {"[gapcheck]\nmethod = \"scan\"\n", "gapcheck.method"},
		"unknown watchdog key":    {"[watchdog]\nrestart = true\n", "unknown keys"},
		"relative result file":    {"[gapcheck]\nresult_file = \"r.json\"\n", "gapcheck.result_file"},
		"relative galexie conf":   {"[gapcheck]\ngalexie_config = \"g.toml\"\n", "gapcheck.galexie_config"},
		"checkpoint poll too low": {"[bootstrap]\ncheckpoint_poll = \"1ms\"\n", "bootstrap.checkpoint_poll"},
	} {
		_, err := Load(writeFile(t, "worker.toml", tc.toml, 0o600))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", name, err, tc.want)
		}
	}
}
