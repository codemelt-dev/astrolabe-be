package explorer

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

func realLedger(t testing.TB, l sep54test.RealLedger) sequencer.Ledger {
	t.Helper()
	obj := sep54test.RealObject(l.Seq)
	lcm, h, err := sep54.DecodeLedger(l.Seq, obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	return sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceLake, Metadata: obj.Metadata, Size: len(obj.Body)}
}

func checkJSON(t *testing.T, what, s string) {
	t.Helper()
	if !json.Valid([]byte(s)) {
		t.Fatalf("%s is not valid JSON: %s", what, s)
	}
	if strings.Contains(s, `\u0000`) {
		t.Fatalf("%s contains \\u0000 (jsonb rejects it): %s", what, s)
	}
}

// The clickhouse pipeline's processLedger on real pubnet ledgers: counts
// match the header, keys follow the index conventions and are unique, and
// every JSON value is valid jsonb.
func TestProcessRealLedgers(t *testing.T) {
	for _, rl := range sep54test.RealLedgers() {
		l := realLedger(t, rl)
		p, err := ProcessHistory(network.PublicNetworkPassphrase, l)
		if err != nil {
			t.Fatal(err)
		}
		if p.State != nil {
			t.Fatal("the history processor extracted state changes")
		}
		seq := rl.Seq
		lr := p.Ledger
		if lr.Sequence != seq || lr.Hash != l.Header.Hash || lr.PrevHash != l.Header.PreviousHash ||
			lr.ProtocolVersion != 28 || lr.Source != SourceGalexie || lr.CoreVersion == "" || lr.SorobanStateSize == nil {
			t.Fatalf("ledger row %+v", lr)
		}
		if int(lr.TxCount) != l.LCM.CountTransactions() || len(p.Transactions) != int(lr.TxCount) {
			t.Fatalf("tx count %d, rows %d, set %d", lr.TxCount, len(p.Transactions), l.LCM.CountTransactions())
		}
		var failed, ops, feeBumps int
		for i, tx := range p.Transactions {
			if tx.TxIndex != uint32(i+1) || tx.ID != toid.New(int32(seq), int32(i+1), 0).ToInt64() || tx.LedgerSequence != seq {
				t.Fatalf("tx %d: index %d id %d", i, tx.TxIndex, tx.ID)
			}
			if !tx.Successful {
				failed++
			}
			ops += int(tx.OpCount)
			if !strings.HasPrefix(tx.SourceAccount, "G") || !strings.HasPrefix(tx.FeeAccount, "G") ||
				(tx.InnerHash == nil && tx.FeeAccount != tx.SourceAccount) {
				t.Fatalf("tx %d: source %s fee account %s inner %v", i, tx.SourceAccount, tx.FeeAccount, tx.InnerHash)
			}
			if tx.InnerHash != nil {
				feeBumps++
			}
		}
		successful := map[uint32]bool{}
		soroban := map[uint32]bool{}
		if failed != int(lr.FailedTxCount) || ops != int(lr.OpCount) || len(p.Operations) != ops {
			t.Fatalf("failed %d/%d ops %d/%d/%d", failed, lr.FailedTxCount, ops, lr.OpCount, len(p.Operations))
		}

		// Transaction hashes are the network hashes of the envelopes.
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, l.LCM)
		if err != nil {
			t.Fatal(err)
		}
		wantEvents := 0
		for i := 0; ; i++ {
			tx, err := reader.Read()
			if err == io.EOF {
				break
			}
			h, _ := network.HashTransactionInEnvelope(tx.Envelope, network.PublicNetworkPassphrase)
			if p.Transactions[i].Hash != h {
				t.Fatalf("tx %d hash mismatch", i)
			}
			successful[tx.Index], soroban[tx.Index] = tx.Successful(), tx.IsSorobanTx()
			evs, _ := tx.GetTransactionEvents()
			wantEvents += len(evs.TransactionEvents)
			for _, list := range evs.OperationEvents {
				wantEvents += len(list)
			}
		}

		types := map[int32]int{}
		seen := map[[3]int64]bool{}
		returns, failedOps := 0, map[uint32]bool{}
		for _, op := range p.Operations {
			k := [3]int64{int64(op.LedgerSequence), int64(op.TxIndex), int64(op.OpIndex)}
			if seen[k] {
				t.Fatalf("duplicate operation key %v", k)
			}
			seen[k] = true
			if op.ID != toid.New(int32(seq), int32(op.TxIndex), int32(op.OpIndex+1)).ToInt64() {
				t.Fatalf("operation %v id %d", k, op.ID)
			}
			checkJSON(t, "details", op.Details)
			if op.Type == int32(xdr.OperationTypeInvokeHostFunction) &&
				(!strings.HasPrefix(op.Details, `{"host_function":`) || !strings.Contains(op.Details, `"auth":[`) || strings.Contains(op.Details, "root_invocation")) {
				t.Fatalf("invoke_host_function details not slim: %.300s", op.Details)
			}
			types[op.Type]++
			// Every operation of a successful transaction succeeded; a failed
			// one's operations either all have no result (it failed before
			// they ran) or at least one failed.
			switch {
			case strings.HasPrefix(op.ResultCode, "op_") || op.ResultCode == "":
				if successful[op.TxIndex] {
					t.Fatalf("operation %v of a successful transaction: result %q", k, op.ResultCode)
				}
				failedOps[op.TxIndex] = true
			case !strings.HasSuffix(op.ResultCode, "_success"):
				if successful[op.TxIndex] {
					t.Fatalf("operation %v of a successful transaction: result %q", k, op.ResultCode)
				}
				failedOps[op.TxIndex] = true
			}
			if op.ResultCode == "op_inner" {
				t.Fatalf("operation %v: result code not resolved", k)
			}
			// A successful Soroban transaction's call returned a value.
			if op.ReturnValue != "" {
				checkJSON(t, "return value", op.ReturnValue)
				returns++
			}
			if (op.ReturnValue != "") != (successful[op.TxIndex] && soroban[op.TxIndex]) {
				t.Fatalf("operation %v: return value %q (successful %v, soroban %v)", k, op.ReturnValue, successful[op.TxIndex], soroban[op.TxIndex])
			}
		}
		for idx, ok := range successful {
			if !ok && !failedOps[idx] {
				t.Fatalf("tx %d failed but every operation succeeded", idx)
			}
		}
		if returns == 0 {
			t.Fatal("the fixtures have successful Soroban calls")
		}
		if feeBumps == 0 {
			t.Fatal("the fixtures have fee bumps")
		}
		t.Logf("ledger %d: %d tx (%d failed), %d ops of %d types, %d events, %d transfers, %d activity rows, %d calls, %d creations, ~%d KiB prepared",
			seq, len(p.Transactions), failed, len(p.Operations), len(types), len(p.Events), len(p.Transfers), len(p.Activity),
			len(p.Calls), len(p.Creations), p.Size()>>10)

		if len(p.Events) != wantEvents {
			t.Fatalf("%d event rows, want %d", len(p.Events), wantEvents)
		}
		evKeys := map[[4]int64]bool{}
		feeEvents := 0
		for _, e := range p.Events {
			k := [4]int64{int64(e.LedgerSequence), int64(e.TxIndex), int64(e.OpIndex), int64(e.EventIndex)}
			if evKeys[k] {
				t.Fatalf("duplicate event key %v", k)
			}
			evKeys[k] = true
			if (e.OpIndex == -1) != (e.Stage != StageOperation) {
				t.Fatalf("event %v: stage %s", k, e.Stage)
			}
			if e.Stage == StageFee {
				feeEvents++
			}
			for _, topic := range e.Topics { // every topic: pubnet has events with more than 4
				checkJSON(t, "topic", topic)
			}
			checkJSON(t, "data", e.Data)
		}
		if feeEvents != len(p.Transactions) {
			t.Fatalf("%d fee events for %d transactions", feeEvents, len(p.Transactions))
		}

		groups := map[[2]int64]uint32{}
		fees := 0
		for _, tr := range p.Transfers {
			g := [2]int64{int64(tr.TxIndex), int64(tr.OpIndex)}
			if tr.Position != groups[g] {
				t.Fatalf("transfer %v position %d, want %d", g, tr.Position, groups[g])
			}
			groups[g]++
			if tr.Kind == "fee" {
				fees++
				if tr.OpIndex != -1 || tr.Asset != "native" {
					t.Fatalf("fee transfer %+v", tr)
				}
			}
			if tr.TxIndex < 1 || int(tr.TxIndex) > len(p.Transactions) || tr.ContractID == "" || tr.Amount == "" {
				t.Fatalf("transfer %+v", tr)
			}
			if tr.ToMuxedInfo != "" {
				checkJSON(t, "muxed info", tr.ToMuxedInfo)
			}
		}
		if fees < len(p.Transactions) {
			t.Fatalf("%d fee transfers for %d transactions", fees, len(p.Transactions))
		}

		// Activity: one row per address and transaction, sorted, with at
		// least the roles the envelope and the transfers give.
		roles := map[[2]string]uint8{}
		for i, a := range p.Activity {
			key := [2]string{fmt.Sprint(a.TxIndex), a.Account}
			if _, dup := roles[key]; dup || a.Roles == 0 || !(strings.HasPrefix(a.Account, "G") || strings.HasPrefix(a.Account, "C")) {
				t.Fatalf("activity row %+v", a)
			}
			if i > 0 && (p.Activity[i-1].TxIndex > a.TxIndex || p.Activity[i-1].TxIndex == a.TxIndex && p.Activity[i-1].Account >= a.Account) {
				t.Fatalf("activity not ordered at %d", i)
			}
			roles[key] = a.Roles
		}
		has := func(tx uint32, account string, role uint8) bool {
			return roles[[2]string{fmt.Sprint(tx), account}]&role != 0
		}
		for _, tx := range p.Transactions {
			if !has(tx.TxIndex, tx.SourceAccount, RoleSource|RoleChanged) || !has(tx.TxIndex, tx.FeeAccount, RoleFeePayer) {
				t.Fatalf("tx %d: source %s or fee payer %s missing from the activity", tx.TxIndex, tx.SourceAccount, tx.FeeAccount)
			}
		}
		for _, op := range p.Operations {
			if !has(op.TxIndex, op.SourceAccount, RoleOpSource) {
				t.Fatalf("operation %d/%d: source missing from the activity", op.TxIndex, op.OpIndex)
			}
		}
		for _, tr := range p.Transfers {
			for _, side := range []string{tr.From, tr.To} {
				if (strings.HasPrefix(side, "G") || strings.HasPrefix(side, "C")) && !has(tr.TxIndex, side, RoleTransfer) {
					t.Fatalf("transfer %+v: %s missing from the activity", tr, side)
				}
			}
		}

		// One call per invoke_contract operation, with its outcome.
		calls := 0
		for _, op := range p.Operations {
			if op.Type == int32(xdr.OperationTypeInvokeHostFunction) && strings.HasPrefix(op.Details, `{"host_function":{"invoke_contract"`) {
				calls++
			}
		}
		if len(p.Calls) != calls || calls == 0 {
			t.Fatalf("%d calls for %d invoke_contract operations", len(p.Calls), calls)
		}
		for _, c := range p.Calls {
			if !strings.HasPrefix(c.ContractID, "C") || c.Function == "" || !strings.HasPrefix(c.Caller, "G") ||
				c.Successful != successful[c.TxIndex] || (c.ReturnValue != "") != c.Successful {
				t.Fatalf("call %+v", c)
			}
		}
		for _, c := range p.Creations {
			if !strings.HasPrefix(c.ContractID, "C") || !strings.HasPrefix(c.Creator, "G") || !successful[c.TxIndex] ||
				(c.Executable == "wasm") != (len(c.WasmHash) == 32) {
				t.Fatalf("creation %+v", c)
			}
		}

		// Pure: processing again yields identical rows.
		again, err := ProcessHistory(network.PublicNetworkPassphrase, l)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(p, again) {
			t.Fatal("processLedger is not deterministic")
		}
		// Every row has as many values as its table has columns.
		for i, tbl := range historyTables {
			rs := p.rows(i)
			for j := range rs.Len() {
				if n := len(rs.Values(j)); n != len(tbl.cols) {
					t.Fatalf("%s row %d has %d values for %d columns", tbl.name, j, n, len(tbl.cols))
				}
			}
		}
	}
}

// Each pipeline's processor extracts only what its store keeps: the
// explorer pipeline the ledger row and the state changes, reindex the ledger
// row alone.
func TestProcessModes(t *testing.T) {
	l := realLedger(t, sep54test.RealLedgers()[0])
	history, err := ProcessHistory(network.PublicNetworkPassphrase, l)
	if err != nil {
		t.Fatal(err)
	}
	state, err := Process(network.PublicNetworkPassphrase, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.State) == 0 || state.Transactions != nil || state.Operations != nil || state.Activity != nil || state.Transfers != nil ||
		!reflect.DeepEqual(state.Ledger, history.Ledger) || state.Size() >= history.Size() {
		t.Fatalf("explorer processor: %d state changes, %d transactions, size %d (history %d)",
			len(state.State), len(state.Transactions), state.Size(), history.Size())
	}
	ledger, err := LedgerProcessor(network.PublicNetworkPassphrase)(l)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.State != nil || ledger.Transactions != nil || !reflect.DeepEqual(ledger.Ledger, history.Ledger) || ledger.RowCounts()["ledgers"] != 1 {
		t.Fatalf("ledger processor: %+v", ledger)
	}
}

// The AWS lake is another producer: its ledgers are recorded as such.
func TestProcessSourceAndPassphrase(t *testing.T) {
	l := realLedger(t, sep54test.RealLedgers()[0])
	l.Source, l.Metadata = sep54.SourceAWS, nil
	p, err := Process(network.PublicNetworkPassphrase, l)
	if err != nil {
		t.Fatal(err)
	}
	if p.Ledger.Source != SourceAWSLake || p.Ledger.CoreVersion != "" {
		t.Fatalf("ledger row %+v", p.Ledger)
	}
	// The wrong network: the envelopes hash differently, so the reader
	// cannot match them to their results.
	if _, err := ProcessHistory(network.TestNetworkPassphrase, l); err == nil {
		t.Fatal("processing a pubnet ledger with the testnet passphrase succeeded")
	}
}

func BenchmarkProcess(b *testing.B) {
	l := realLedger(b, sep54test.RealLedgers()[0])
	for _, mode := range []struct {
		name string
		p    func(string, sequencer.Ledger) (*Prepared, error)
	}{{"state", Process}, {"history", ProcessHistory}} {
		b.Run(mode.name, func(b *testing.B) {
			for range b.N {
				if _, err := mode.p(network.PublicNetworkPassphrase, l); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
