package sep54_test

import (
	"context"
	"errors"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// Opt-in network tests against the AWS public lake.
//
//	WORKER_TEST_AWS_LEDGERS=2000   decode and validate the newest N ledgers (M2 acceptance)
//	WORKER_TEST_LAKE_CACHE=/path   keep downloaded ledgers there (default: a temp dir);
//	                               a complete cache is reused instead of downloading
const (
	envAWSLedgers = "WORKER_TEST_AWS_LEDGERS"
	envLakeCache  = "WORKER_TEST_LAKE_CACHE"
)

func awsLedgerCount(t *testing.T) int {
	t.Helper()
	v := os.Getenv(envAWSLedgers)
	if v == "" {
		t.Skipf("set %s=N to decode the newest N AWS-lake ledgers (network)", envAWSLedgers)
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 2 {
		t.Fatalf("%s=%q: want a count ≥ 2", envAWSLedgers, v)
	}
	return n
}

// TestAWSLakeRecentLedgers is the M2 acceptance: decode and validate recent
// AWS-lake ledgers; the self-hash and the hash chain must pass for every one,
// the header-only decode must agree with the full decode, and the 9
// metadata keys must describe the ledger.
func TestAWSLakeRecentLedgers(t *testing.T) {
	n := awsLedgerCount(t)
	ctx := context.Background()
	src, err := sep54.OpenAWSLake(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.LoadSchema(ctx, network.PublicNetworkPassphrase); err != nil {
		t.Fatalf("AWS lake manifest: %v", err)
	}
	m, err := src.GetObject(ctx, sep54.ManifestFile)
	if err != nil || string(m.Body) != pubnetManifest {
		t.Fatalf("AWS lake manifest %q %v", m.Body, err)
	}
	tip, err := src.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.GetLedger(ctx, tip+100_000); !errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("future ledger: %v", err)
	}

	dir := os.Getenv(envLakeCache)
	if dir == "" {
		dir = t.TempDir()
	}
	start, end, err := sep54test.CacheRange(dir)
	if err != nil || end-start+1 < uint32(n) {
		end = tip
		start = end - uint32(n) + 1
		t.Logf("downloading ledgers %d-%d from the AWS lake into %s", start, end, dir)
		began := time.Now()
		if err := sep54test.FillCache(ctx, src, dir, start, end, 16); err != nil {
			t.Fatal(err)
		}
		t.Logf("downloaded %d ledgers in %v", n, time.Since(began).Round(time.Millisecond))
	} else {
		start = end - uint32(n) + 1
		t.Logf("reusing cached ledgers %d-%d from %s (AWS tip is %d)", start, end, dir, tip)
	}

	results := make([]ledgerCheck, n)
	var wg sync.WaitGroup
	next := make(chan int)
	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				results[i] = validateCached(dir, start+uint32(i))
			}
		}()
	}
	began := time.Now()
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	elapsed := time.Since(began)

	var sizes, decoded []int
	var fullTotal, headerTotal time.Duration
	protocols := map[uint32]int{}
	txs := 0
	for i, r := range results {
		seq := start + uint32(i)
		if r.err != nil {
			t.Fatalf("ledger %d: %v", seq, r.err)
		}
		if i > 0 {
			if err := sep54.CheckChain(results[i-1].hdr, r.hdr); err != nil {
				t.Fatalf("chain: %v", err)
			}
		}
		sizes = append(sizes, r.size)
		decoded = append(decoded, r.decoded)
		fullTotal += r.fullDur
		headerTotal += r.headerDur
		protocols[r.hdr.ProtocolVersion]++
		txs += r.txs
	}
	slices.Sort(sizes)
	slices.Sort(decoded)
	mean := func(v []int) int {
		s := 0
		for _, x := range v {
			s += x
		}
		return s / len(v)
	}
	last := results[n-1].hdr
	t.Logf("validated %d ledgers %d-%d: self-hash and chain pass; header-only == full decode; LCM V2; metadata consistent", n, start, end)
	t.Logf("compressed bytes: mean %d, p99 %d, max %d; decompressed: mean %d, max %d", mean(sizes), sizes[n*99/100], sizes[n-1], mean(decoded), decoded[n-1])
	t.Logf("full decode %v/ledger, header-only %v/ledger (CPU time per worker); wall %v for all on %d workers; %.0f transactions/ledger; protocols %v",
		(fullTotal / time.Duration(n)).Round(time.Microsecond), (headerTotal / time.Duration(n)).Round(time.Microsecond),
		elapsed.Round(time.Millisecond), runtime.GOMAXPROCS(0), float64(txs)/float64(n), protocols)
	t.Logf("newest validated ledger %d closed %v ago (AWS tip %d)", last.Sequence, time.Since(time.Unix(last.CloseTime, 0)).Round(time.Second), tip)
}

// ledgerCheck is the outcome of validating one cached ledger.
type ledgerCheck struct {
	hdr                sep54.Header
	size, decoded      int
	txs                int
	fullDur, headerDur time.Duration
	err                error
}

func validateCached(dir string, seq uint32) (r ledgerCheck) {
	o, err := sep54test.CacheObject(dir, seq)
	if err != nil {
		r.err = err
		return
	}
	r.size = len(o.Body)
	t0 := time.Now()
	hdr, err := sep54.CheckObject(seq, o)
	r.headerDur = time.Since(t0)
	if err != nil {
		r.err = err
		return
	}
	t0 = time.Now()
	lcm, full, err := sep54.DecodeLedger(seq, o.Body)
	r.fullDur = time.Since(t0)
	if err != nil {
		r.err = err
		return
	}
	if full != hdr {
		r.err = errors.New("header-only decode disagrees with the full decode")
		return
	}
	raw, _ := sep54.Decompress(o.Body)
	r.decoded = len(raw)
	r.txs = lcm.CountTransactions()
	md, err := o.LedgerMetadata()
	if err != nil {
		r.err = err
		return
	}
	s := strconv.FormatUint(uint64(seq), 10)
	want := map[string]string{
		"start-ledger":            s,
		"end-ledger":              s,
		"start-ledger-close-time": strconv.FormatInt(full.CloseTime, 10),
		"end-ledger-close-time":   strconv.FormatInt(full.CloseTime, 10),
		"protocol-version":        strconv.FormatUint(uint64(full.ProtocolVersion), 10),
		"network-passphrase":      network.PublicNetworkPassphrase,
		"compression-type":        "zst",
	}
	for k, v := range want {
		if md[k] != v {
			r.err = errors.New("metadata " + k + " = " + md[k] + ", header says " + v)
			return
		}
	}
	r.hdr = full
	return r
}
