package archive_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"testing"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake/laketest"
)

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// The smoke test run against the lake as a stand-in provider (plain HTTP,
// root keys): every provider-compatibility check passes except the two
// policy checks, because the lake's root key may delete. At the real
// provider, with the uploader key and the archive policy, all must pass.
func TestSmokeAgainstLakeStandIn(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	for _, pathStyle := range []bool{false, true} {
		checks := archive.Smoke(context.Background(), archive.SmokeOptions{
			Creds: config.ArchiveCredentials{
				Endpoint:  l.S3Endpoint(),
				Region:    l.Cfg.Region,
				AccessKey: l.Creds.AccessKey,
				SecretKey: l.Creds.SecretKey,
			},
			Bucket:    l.Cfg.Bucket(),
			PathStyle: pathStyle,
			Log:       slog.New(slog.NewTextHandler(testWriter{t}, nil)),
		})
		var names []string
		for _, c := range checks {
			names = append(names, c.Name)
			t.Logf("path_style=%v %-26s pass=%v %s", pathStyle, c.Name, c.Pass, c.Detail)
		}
		want := []string{"list", "put-sdk-default-checksum", "put-crc32c", "etag-md5", "get-checksum-mode", "head",
			"conditional-put-412", "list-start-after", "sdk-datastore", "delete-denied", "delete-missing-denied",
			"path-style", "virtual-hosted"}
		if !slices.Equal(names, want) {
			t.Fatalf("checks %v", names)
		}
		if failed := archive.Failed(checks); !slices.Equal(failed, []string{"delete-denied", "delete-missing-denied"}) {
			t.Fatalf("path_style=%v: failed %v, want only the delete-policy checks", pathStyle, failed)
		}
		for _, c := range checks {
			if c.Name == "delete-denied" && !strings.Contains(c.Detail, "the uploader deleted") {
				t.Fatalf("delete-denied detail %q", c.Detail)
			}
			if c.Name == "put-crc32c" && !strings.Contains(c.Detail, "plain HTTP") {
				t.Fatalf("put-crc32c detail %q", c.Detail)
			}
		}
	}

	// Over HTTPS (a TLS proxy in front of the lake) the SDK sends the CRC32C
	// as an aws-chunked trailer with an unsigned streaming payload, and the
	// lake stores and returns the same checksum.
	target, _ := url.Parse(l.S3Endpoint())
	proxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(target))
	defer proxy.Close()
	checks := archive.Smoke(context.Background(), archive.SmokeOptions{
		Creds:      config.ArchiveCredentials{Endpoint: proxy.URL, Region: l.Cfg.Region, AccessKey: l.Creds.AccessKey, SecretKey: l.Creds.SecretKey},
		Bucket:     l.Cfg.Bucket(),
		HTTPClient: proxy.Client(),
		Log:        slog.New(slog.NewTextHandler(testWriter{t}, nil)),
	})
	if failed := archive.Failed(checks); !slices.Equal(failed, []string{"delete-denied", "delete-missing-denied"}) {
		t.Fatalf("over HTTPS: failed %v", failed)
	}
	for _, c := range checks {
		if strings.HasPrefix(c.Name, "put-") {
			t.Logf("HTTPS %s: %s", c.Name, c.Detail)
			if !strings.Contains(c.Detail, "aws-chunked trailer") || !strings.Contains(c.Detail, "STREAMING-UNSIGNED-PAYLOAD-TRAILER") {
				t.Fatalf("%s over HTTPS: %s", c.Name, c.Detail)
			}
		}
	}

	// The probe mode runs the single check that is safe on the archive bucket.
	checks = archive.Smoke(context.Background(), archive.SmokeOptions{
		Creds:           config.ArchiveCredentials{Endpoint: l.S3Endpoint(), Region: l.Cfg.Region, AccessKey: l.Creds.AccessKey, SecretKey: l.Creds.SecretKey},
		Bucket:          l.Cfg.Bucket(),
		DeleteProbeOnly: true,
	})
	if len(checks) != 1 || checks[0].Name != "delete-missing-denied" || checks[0].Pass {
		t.Fatalf("probe-only: %+v", checks)
	}
}
