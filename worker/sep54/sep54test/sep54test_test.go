package sep54test

import (
	"testing"
	"time"

	"stellar-explorer/worker/sep54"
)

func TestRealLedgersChain(t *testing.T) {
	ls := RealLedgers()
	if len(ls) < 2 {
		t.Fatalf("want at least 2 fixtures, have %d", len(ls))
	}
	var prev sep54.Header
	for i, l := range ls {
		_, h, err := sep54.DecodeLedger(l.Seq, l.Body)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err := sep54.CheckChain(prev, h); err != nil {
				t.Fatal(err)
			}
		}
		prev = h
		if o := RealObject(l.Seq); sep54.VerifyCRC32C(o.Body, o.ChecksumCRC32C) != nil || len(o.Metadata) != len(sep54.MetadataKeys) {
			t.Fatalf("RealObject(%d): %+v", l.Seq, o.Metadata)
		}
	}
}

func TestRechain(t *testing.T) {
	start := time.Now()
	objs := Rechain(1000, 5)
	t.Logf("re-chained 5 ledgers in %s", time.Since(start))
	var prev sep54.Header
	for i, o := range objs {
		seq := uint32(1000 + i)
		if o.Key != sep54.ObjectPath(seq) || o.Metadata["start-ledger"] != o.Metadata["end-ledger"] {
			t.Fatalf("object %d: key %s metadata %v", seq, o.Key, o.Metadata)
		}
		lcm, h, err := sep54.DecodeLedger(seq, o.Body)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err := sep54.CheckChain(prev, h); err != nil {
				t.Fatal(err)
			}
		}
		if want := RealLedgers()[i%len(RealLedgers())].LCM.CountTransactions(); lcm.CountTransactions() != want {
			t.Fatalf("ledger %d: %d transactions, want %d", seq, lcm.CountTransactions(), want)
		}
		prev = h
	}
	again := Rechain(1000, 2)
	if string(again[1].Body) != string(objs[1].Body) {
		t.Fatal("Rechain is not deterministic")
	}
}
