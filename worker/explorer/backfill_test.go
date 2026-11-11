package explorer

import (
	"encoding/json"
	"flag"
	"os"
	"reflect"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

var updateHistoryRows = flag.Bool("update", false, "rewrite testdata/history_rows.json from the fixtures")

// historyRows are a history fixture's golden counts.
type historyRows struct {
	Protocol     uint32 `json:"protocol"`
	LCM          int32  `json:"lcm"`
	Transactions int    `json:"transactions"`
	Operations   int    `json:"operations"`
	Events       int    `json:"events"`
	Transfers    int    `json:"transfers"`
	Activity     int    `json:"activity"`
	Calls        int    `json:"calls"`
	Creations    int    `json:"creations"`
	Trades       int    `json:"trades"`
}

// The history before go-live, as `worker clickhouse-backfill` processes it:
// a ledger of every protocol that ran on pubnet, in the AWS lake's form
// (LedgerCloseMeta V0, V1 or V2, with transaction meta V4 throughout).
// Each decodes with the history decoder only, processes with the live
// pipeline's processor, matches its own header's counts and its golden
// row counts, passes the SDK's check of transfers against balance changes,
// and has trades that match its token movements.
func TestProcessHistoryFixtures(t *testing.T) {
	golden := map[uint32]historyRows{}
	if raw, err := os.ReadFile("testdata/history_rows.json"); err == nil {
		if err := json.Unmarshal(raw, &golden); err != nil {
			t.Fatal(err)
		}
	}
	got := map[uint32]historyRows{}
	protocols := map[uint32]bool{}
	var v0Trades, bookTrades, poolTrades, inflation, tokenTransfers, sorobanBefore23, feeBumps, failed int
	for _, rl := range sep54test.HistoryLedgers() {
		lcm, h, err := sep54.DecodeHistoryLedger(rl.Seq, rl.Body)
		if err != nil {
			t.Fatalf("ledger %d: %v", rl.Seq, err)
		}
		// The live paths refuse everything before protocol 23.
		if _, _, err := sep54.DecodeLedger(rl.Seq, rl.Body); (err == nil) != (h.LCMVersion == 2) {
			t.Errorf("ledger %d (LedgerCloseMeta V%d): the live decoder says %v", rl.Seq, h.LCMVersion, err)
		}
		if want := map[bool]int32{true: 0, false: 1}[h.ProtocolVersion < 20]; h.ProtocolVersion < 23 && h.LCMVersion != want {
			t.Errorf("ledger %d: protocol %d with LedgerCloseMeta V%d", rl.Seq, h.ProtocolVersion, h.LCMVersion)
		}
		l := sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceAWS, Size: len(rl.Body)}
		p, err := ProcessHistory(network.PublicNetworkPassphrase, l)
		if err != nil {
			t.Fatalf("ledger %d (protocol %d): %v", rl.Seq, h.ProtocolVersion, err)
		}
		if p.Ledger.Source != SourceAWSLake || p.Ledger.ProtocolVersion != h.ProtocolVersion {
			t.Errorf("ledger %d row: %+v", rl.Seq, p.Ledger)
		}
		if len(p.Transactions) != int(p.Ledger.TxCount) || len(p.Operations) != int(p.Ledger.OpCount) || len(p.Transactions) == 0 {
			t.Errorf("ledger %d: %d transactions and %d operations, the header counts %d and %d", rl.Seq,
				len(p.Transactions), len(p.Operations), p.Ledger.TxCount, p.Ledger.OpCount)
		}
		if err := token_transfer.VerifyEvents(lcm, network.PublicNetworkPassphrase, true); err != nil {
			t.Errorf("ledger %d: transfers don't match the balance changes: %v", rl.Seq, err)
		}
		if err := CheckTrades(p); err != nil {
			t.Errorf("trades don't match the token movements: %v", err)
		}
		// The trades-only processor finds the same trades, and nothing else.
		only, err := TradesProcessor(network.PublicNetworkPassphrase)(l)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(only.Trades, p.Trades) || only.Transactions != nil || only.Transfers != nil || !reflect.DeepEqual(only.Ledger, p.Ledger) {
			t.Errorf("ledger %d: the trades-only processor differs", rl.Seq)
		}

		protocols[h.ProtocolVersion] = true
		got[rl.Seq] = historyRows{Protocol: h.ProtocolVersion, LCM: h.LCMVersion, Transactions: len(p.Transactions), Operations: len(p.Operations),
			Events: len(p.Events), Transfers: len(p.Transfers), Activity: len(p.Activity), Calls: len(p.Calls), Creations: len(p.Creations),
			Trades: len(p.Trades)}
		successful := map[uint32]bool{}
		for _, tx := range p.Transactions {
			successful[tx.TxIndex] = tx.Successful
			if !tx.Successful {
				failed++
			}
			if tx.InnerHash != nil {
				feeBumps++
			}
			if tx.Soroban && h.ProtocolVersion < 23 {
				sorobanBefore23++
			}
		}
		for _, tr := range p.Trades {
			if !successful[tr.TxIndex] || tr.SoldAsset == tr.BoughtAsset || tr.Buyer == "" || tr.Seller == "" ||
				tr.SoldAmount < 0 || tr.BoughtAmount < 0 || tr.SoldAmount+tr.BoughtAmount == 0 {
				t.Errorf("ledger %d trade %+v", rl.Seq, tr)
			}
			switch {
			case tr.Venue == VenuePool && tr.Seller[0] == 'L' && tr.OfferID == 0 && h.ProtocolVersion >= 18:
				poolTrades++
			case tr.Venue == VenueOrderBook && tr.Seller[0] == 'G' && tr.OfferID > 0:
				if h.ProtocolVersion < 18 {
					v0Trades++
				} else {
					bookTrades++
				}
			default:
				t.Errorf("ledger %d trade %+v", rl.Seq, tr)
			}
		}
		for _, op := range p.Operations {
			if op.Type == int32(xdr.OperationTypeInflation) && op.Successful {
				inflation++
			}
		}
		for _, tr := range p.Transfers {
			if tr.Asset == "" && tr.Kind != "fee" && h.ProtocolVersion < 23 {
				tokenTransfers++
			}
		}
	}

	// Every protocol that ran on pubnet before go-live: 14 never did.
	for v := uint32(1); v <= 29; v++ {
		if protocols[v] != (v != 14) {
			t.Errorf("protocol %d: fixture %v", v, protocols[v])
		}
	}
	for what, n := range map[string]int{"trades before protocol 18": v0Trades, "order-book trades": bookTrades, "pool trades": poolTrades,
		"inflation operations": inflation, "transfers of non-SAC tokens before protocol 23": tokenTransfers,
		"Soroban transactions before protocol 23": sorobanBefore23, "fee bumps": feeBumps, "failed transactions": failed} {
		if n == 0 {
			t.Errorf("no fixture has %s", what)
		}
	}

	if *updateHistoryRows {
		raw, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("testdata/history_rows.json", append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if !reflect.DeepEqual(got, golden) {
		for seq, g := range got {
			if golden[seq] != g {
				t.Errorf("ledger %d: rows %+v, golden %+v", seq, g, golden[seq])
			}
		}
		t.Errorf("%d fixtures, %d golden: run `go test ./explorer -run TestProcessHistoryFixtures -update` after reviewing", len(got), len(golden))
	}
}
