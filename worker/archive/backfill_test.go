package archive_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// The backfill copies a range just below the archive's oldest ledger, 1:1
// and chained to it, across a partition boundary. A rerun skips the
// finished partitions; without the state file, every upload answers 412
// with the same bytes.
func TestBackfill(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	chain := sep54test.Chain(127_990, 31) // 127990 … 128020, across 128000
	top := chain[30]
	e.putArchive(top.Key, top.Body, top.Metadata)
	arch, err := e.openArchive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	src := sep54test.NewMemSource(chain[:30]...)
	state := filepath.Join(t.TempDir(), "backfill.json")
	opts := archive.BackfillOptions{Source: src, Archive: arch, From: 127_990, To: 128_019, Workers: 4, Rate: 1000, State: state, Log: quiet}

	st, err := archive.Backfill(ctx, opts)
	if err != nil || st.Partitions != 2 || st.Skipped != 0 || st.Created != 30 || st.Bytes == 0 {
		t.Fatalf("backfill: %+v %v", st, err)
	}
	for _, o := range chain[:30] {
		got, err := arch.GetObject(ctx, o.Key)
		if err != nil || !bytes.Equal(got.Body, o.Body) || !maps.Equal(got.Metadata, o.Metadata) || got.ContentType != o.ContentType {
			t.Fatalf("%s: %v", o.Key, err)
		}
	}

	gets := src.Gets.Load()
	if st, err := archive.Backfill(ctx, opts); err != nil || st.Skipped != 2 || st.Created+st.Existed != 0 || src.Gets.Load() != gets {
		t.Fatalf("rerun: %+v %v, %d reads", st, err, src.Gets.Load()-gets)
	}

	if err := os.Remove(state); err != nil {
		t.Fatal(err)
	}
	if st, err := archive.Backfill(ctx, opts); err != nil || st.Existed != 30 || st.Created != 0 {
		t.Fatalf("without state: %+v %v", st, err)
	}
}

// A range that doesn't end just below the archive's ledgers, a ledger the
// source lacks, a broken chain, a chain that doesn't reach the archive's
// and a different ledger in the archive each stop the backfill.
func TestBackfillStops(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	chain := sep54test.Chain(1000, 11) // 1000 … 1010; the archive holds 1010
	e.putArchive(chain[10].Key, chain[10].Body, chain[10].Metadata)
	arch, err := e.openArchive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := func(src archive.LedgerSource, to uint32, dryRun bool) error {
		_, err := archive.Backfill(ctx, archive.BackfillOptions{Source: src, Archive: arch, From: 1000, To: to, Workers: 3, DryRun: dryRun, Log: quiet})
		return err
	}
	wantErr := func(name string, err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}

	wantErr("range", run(sep54test.NewMemSource(chain[:9]...), 1008, true), "read ledger 1009 from the archive")

	missing := sep54test.NewMemSource(chain[:10]...)
	missing.Remove(1004)
	wantErr("missing", run(missing, 1009, true), "the source lacks ledger 1004")

	broken := sep54test.NewMemSource(chain[:10]...)
	broken.Add(sep54test.Chain(1005, 1)...) // the same ledger number, another chain
	wantErr("broken", run(broken, 1009, true), "previous hash")

	other := sep54test.NewMemSource(sep54test.Chain(999, 11)[1:]...) // 1000 … 1009 of another chain
	wantErr("unanchored", run(other, 1009, true), "does not chain to ledger 1010 in the archive")

	if _, err := arch.GetLedger(ctx, 1000); !errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("a dry run uploaded: %v", err)
	}

	stranger := sep54test.Chain(1003, 1)[0]
	e.putArchive(stranger.Key, stranger.Body, stranger.Metadata)
	wantErr("different", run(sep54test.NewMemSource(chain[:10]...), 1009, false), "the archive holds a different ledger 1003")
}
