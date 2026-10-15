package prune

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeTip serves a model lake's /v1/tip.
type fakeTip struct {
	mu          sync.Mutex
	tip, oldest *uint32
	err         error
}

func (f *fakeTip) Get(context.Context) (sep54.Tip, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return sep54.Tip{}, f.err
	}
	var t, o *uint32
	if f.tip != nil {
		v := *f.tip
		t = &v
	}
	if f.oldest != nil {
		v := *f.oldest
		o = &v
	}
	return sep54.Tip{Tip: t, Oldest: o}, nil
}

func u32(v uint32) *uint32 { return &v }

type alerts struct {
	mu    sync.Mutex
	kinds []string
}

func (a *alerts) alert(kind, msg string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind)
}

func (a *alerts) count(kind string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, k := range a.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

// scenario is a random lake state and pruning configuration.
type scenario struct {
	tip, oldest  uint32
	archiveTip   uint32
	archiveKnown bool
	w            uint32
	cursors      []Cursor
	used         float64
	diskKnown    bool
	watermark    float64
}

func randomScenario(r *rand.Rand) scenario {
	s := scenario{watermark: 0.8}
	s.tip = 1 + r.Uint32N(5_000)
	s.oldest = 1 + r.Uint32N(s.tip)
	s.archiveKnown = r.IntN(5) > 0
	lo := int64(s.oldest) - 10
	s.archiveTip = uint32(max(lo, 0) + int64(r.Uint32N(s.tip-uint32(max(lo, 0))+20)))
	s.w = 1 + r.Uint32N(300)
	if r.IntN(10) == 0 {
		s.w = 0 // the pruner must treat it as 1
	}
	for i := range r.IntN(4) {
		c := Cursor{Pipeline: fmt.Sprintf("p%d", i), Known: r.IntN(8) > 0}
		if c.Known {
			c.Seq = uint32(max(int64(s.oldest)-10, 0) + int64(r.Uint32N(s.tip+20)))
		}
		s.cursors = append(s.cursors, c)
	}
	s.diskKnown = r.IntN(10) > 0
	s.used = r.Float64()
	return s
}

// Property: over thousands of random states, the pruner never deletes an
// unarchived ledger, the newest object or anything within the retention,
// never passes a reader's cursor unless the disk is above the watermark,
// fails closed on unknown inputs, deletes oldest first without skipping,
// and deletes everything the rules allow.
func TestPrunerProperties(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	r := rand.New(rand.NewPCG(seed, 42))
	for i := range 5000 {
		s := randomScenario(r)
		ft := &fakeTip{tip: u32(s.tip), oldest: u32(s.oldest)}
		var deleted []uint32
		var readers []string
		cursors := map[string]Cursor{}
		for _, c := range s.cursors {
			readers = append(readers, c.Pipeline)
			cursors[c.Pipeline] = c
		}
		p, err := New(Options{
			LakeTip:    ft,
			ArchiveTip: func() (uint32, bool) { return s.archiveTip, s.archiveKnown },
			Readers:    readers,
			Cursor:     func(name string) (uint32, bool) { c := cursors[name]; return c.Seq, c.Known },
			Retention:  s.w,
			DiskUsage: func() (float64, error) {
				if !s.diskKnown {
					return 0, errors.New("statfs failed")
				}
				return s.used, nil
			},
			HighWatermark: s.watermark,
			MaxPerPass:    1 << 30,
			Log:           quiet,
			Delete: func(_ context.Context, seq uint32) error {
				// The lake's DeleteObject: deleting the oldest advances it.
				deleted = append(deleted, seq)
				ft.mu.Lock()
				if *ft.oldest == seq {
					*ft.oldest = seq + 1
				}
				ft.mu.Unlock()
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		fail := func(format string, args ...any) {
			t.Fatalf("seed %d case %d %+v: %s (deleted %d: %v…)", seed, i, s, fmt.Sprintf(format, args...), len(deleted), head(deleted))
		}
		w := max(s.w, 1)
		overWatermark := s.diskKnown && s.used > s.watermark
		allKnown := true
		minCursor := ^uint32(0)
		for _, c := range s.cursors {
			allKnown = allKnown && c.Known
			minCursor = min(minCursor, c.Seq)
		}
		for j, seq := range deleted {
			switch {
			case !s.archiveKnown:
				fail("deleted with the archive tip unknown")
			case seq > s.archiveTip:
				fail("deleted unarchived ledger %d", seq)
			case seq >= s.tip:
				fail("deleted the newest object %d", seq)
			case s.tip-seq < w:
				fail("deleted %d within the retention", seq)
			case !overWatermark && (!allKnown || seq > minCursor):
				fail("deleted %d past a reader's cursor below the watermark", seq)
			case seq != s.oldest+uint32(j):
				fail("not oldest first: %d at position %d", seq, j)
			}
			if k := sep54.Key("ledgers/pubnet", seq); func() bool { got, ok := sep54.ParseKey("ledgers/pubnet", k); return !ok || got != seq }() {
				fail("key %q of %d is not a canonical ledger key", k, seq)
			}
		}
		// Completeness: what remains is exactly what the rules keep.
		ft.mu.Lock()
		after := Decide(Inputs{LakeTip: u32(s.tip), LakeOldest: u32(*ft.oldest), ArchiveTip: s.archiveTip, ArchiveKnown: s.archiveKnown,
			Retention: s.w, Cursors: s.cursors, DiskUsed: s.used, DiskKnown: s.diskKnown, HighWatermark: s.watermark})
		ft.mu.Unlock()
		if after.OK {
			fail("ledgers %d-%d are still deletable after the pass", after.From, after.To)
		}
	}
}

func head(s []uint32) []uint32 {
	if len(s) > 5 {
		return s[:5]
	}
	return s
}

// Fail closed: an unreadable lake tip, an empty lake, an unknown archive
// tip or cursor delete nothing; a delete error ends the pass at once.
func TestPrunerFailsClosed(t *testing.T) {
	base := func() (Options, *fakeTip, *[]uint32) {
		ft := &fakeTip{tip: u32(100), oldest: u32(1)}
		var deleted []uint32
		return Options{
			LakeTip:    ft,
			ArchiveTip: func() (uint32, bool) { return 90, true },
			Readers:    []string{"explorer"},
			Cursor:     func(string) (uint32, bool) { return 80, true },
			Retention:  10,
			Log:        quiet,
			Delete: func(_ context.Context, seq uint32) error {
				deleted = append(deleted, seq)
				return nil
			},
		}, ft, &deleted
	}
	o, _, deleted := base()
	p, _ := New(o)
	if n, err := p.Pass(context.Background()); err != nil || n != 80 || (*deleted)[79] != 80 {
		t.Fatalf("baseline: %d %v", n, err)
	}
	for name, mutate := range map[string]func(o *Options, ft *fakeTip){
		"lake tip unavailable": func(o *Options, ft *fakeTip) { ft.err = errors.New("connection refused") },
		"empty lake":           func(o *Options, ft *fakeTip) { ft.tip, ft.oldest = nil, nil },
		"archive tip unknown":  func(o *Options, ft *fakeTip) { o.ArchiveTip = func() (uint32, bool) { return 0, false } },
		"nothing archived":     func(o *Options, ft *fakeTip) { o.ArchiveTip = func() (uint32, bool) { return 0, true } },
		"cursor unknown":       func(o *Options, ft *fakeTip) { o.Cursor = func(string) (uint32, bool) { return 0, false } },
		"cursor unknown, disk unknown": func(o *Options, ft *fakeTip) {
			o.Cursor = func(string) (uint32, bool) { return 0, false }
			o.DiskUsage = func() (float64, error) { return 0, errors.New("statfs") }
			o.HighWatermark = 0.5
		},
		"lake within the retention": func(o *Options, ft *fakeTip) { ft.tip = u32(10) },
	} {
		o, ft, deleted := base()
		mutate(&o, ft)
		p, _ := New(o)
		_, _ = p.Pass(context.Background())
		if len(*deleted) != 0 {
			t.Errorf("%s: deleted %d ledgers", name, len(*deleted))
		}
	}
	o, _, deleted = base()
	o.Delete = func(_ context.Context, seq uint32) error {
		*deleted = append(*deleted, seq)
		if seq == 5 {
			return errors.New("503")
		}
		return nil
	}
	p, _ = New(o)
	if n, err := p.Pass(context.Background()); err == nil || n != 4 || len(*deleted) != 5 || p.Stats().Errors != 1 {
		t.Fatalf("delete error: n %d err %v deleted %v", n, err, *deleted)
	}
}

// The archive-stall alert: the archive tip has not advanced for 15 min
// while the lake tip has; it fires once and re-arms when the archive moves.
func TestPrunerArchiveStallAlert(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	ft := &fakeTip{tip: u32(100), oldest: u32(1)}
	archive := uint32(50)
	al := &alerts{}
	p, _ := New(Options{LakeTip: ft, ArchiveTip: func() (uint32, bool) { return archive, true }, Retention: 1,
		Delete: func(context.Context, uint32) error { return nil }, Alert: al.alert, Log: quiet, Now: func() time.Time { return now }})
	pass := func() { _, _ = p.Pass(context.Background()) }
	pass()
	now = now.Add(16 * time.Minute)
	pass() // the lake has not moved: no alert
	if al.count("archive_stalled") != 0 {
		t.Fatal("alert while the lake tip did not move")
	}
	*ft.tip = 300
	pass()
	pass()
	if al.count("archive_stalled") != 1 {
		t.Fatalf("%d stall alerts", al.count("archive_stalled"))
	}
	archive = 60
	pass()
	now = now.Add(10 * time.Minute)
	*ft.tip = 400
	pass()
	if al.count("archive_stalled") != 1 {
		t.Fatal("alert before 15 min since the archive moved")
	}
	now = now.Add(6 * time.Minute)
	pass()
	if al.count("archive_stalled") != 2 {
		t.Fatalf("%d stall alerts after the re-arm", al.count("archive_stalled"))
	}
}

// Against a real lake: deletes go through the lake's DeleteObject, so its
// oldest index advances; the manifest and VersityGW's directories are
// untouched; the disk watermark lifts the cursor gate (with an alert).
func TestPrunerOnLake(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	ctx := context.Background()
	src, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	chain := sep54test.Chain(13_000_000, 20)
	if _, err := devfeed.Run(ctx, devfeed.Options{Upstream: sep54test.NewMemSource(chain...), Lake: src.DataStore(),
		Passphrase: network.PublicNetworkPassphrase, Start: 13_000_000, End: 13_000_019, Log: quiet}); err != nil {
		t.Fatal(err)
	}
	// VersityGW's own entries and the manifest, which must survive.
	bucketDir := filepath.Join(l.Dir, "s3", l.Cfg.Bucket())
	var keep []string
	for _, dir := range []string{bucketDir, filepath.Join(bucketDir, filepath.FromSlash(l.Cfg.Prefix()))} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if _, ok := sep54.ParsePartitionName(e.Name()); !ok && e.Name() != "ledgers" {
				keep = append(keep, filepath.Join(dir, e.Name()))
			}
		}
	}
	t.Logf("kept entries: %v", keep)
	if len(keep) < 2 {
		t.Fatalf("expected the manifest and VersityGW's directories, found %v", keep)
	}
	tip := sep54.NewTipClient(l.ControlAddr())
	used := 0.5
	al := &alerts{}
	p, err := New(Options{
		Lake:          func(ctx context.Context) (*sep54.Source, error) { return src, nil },
		LakeTip:       tip,
		ArchiveTip:    func() (uint32, bool) { return 13_000_010, true },
		Readers:       []string{"explorer"},
		Cursor:        func(string) (uint32, bool) { return 13_000_007, true },
		Retention:     5,
		DiskUsage:     func() (float64, error) { return used, nil },
		HighWatermark: 0.8,
		Alert:         al.alert,
		Log:           quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := p.Pass(ctx); err != nil || n != 8 {
		t.Fatalf("pass: %d %v", n, err)
	}
	got, err := tip.Get(ctx)
	if err != nil || *got.Oldest != 13_000_008 || *got.Tip != 13_000_019 {
		t.Fatalf("lake tip after pruning: %+v %v", got, err)
	}
	for _, o := range chain {
		seq, _ := sep54.ParseObjectPath(o.Key)
		_, err := src.GetLedger(ctx, seq)
		if gone := errors.Is(err, sep54.ErrNotFound); gone != (seq <= 13_000_007) {
			t.Fatalf("ledger %d: %v", seq, err)
		}
	}
	// Above the watermark the cursor no longer holds pruning back; the
	// archive tip still does.
	used = 0.95
	if n, err := p.Pass(ctx); err != nil || n != 3 || al.count("lake_disk_watermark") != 1 || !p.Stats().Watermark {
		t.Fatalf("watermark pass: %d %v alerts %v", n, err, al.kinds)
	}
	if _, err := src.GetObject(ctx, sep54.ManifestFile); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	for _, k := range keep {
		if _, err := os.Stat(k); err != nil {
			t.Fatalf("pruning removed %s: %v", k, err)
		}
	}
	if st := p.Stats(); st.Deleted != 11 || st.Bound != 13_000_010 {
		t.Fatalf("stats %+v", st)
	}
}
