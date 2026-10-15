package explorer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

// ------------------------------------------------------------ test entries

func testAccountID(n byte) xdr.AccountId {
	var k xdr.Uint256
	k[0], k[31] = n, n
	return xdr.AccountId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &k}
}

func testAddress(n byte) string {
	a := testAccountID(n)
	s, err := a.GetAddress()
	if err != nil {
		panic(err)
	}
	return s
}

func testContractID(n byte) xdr.ContractId {
	var id xdr.ContractId
	id[0], id[31] = 0xc0, n
	return id
}

func accountEntry(n byte, balance int64, lml uint32) xdr.LedgerEntry {
	signerKey := xdr.SignerKey{Type: xdr.SignerKeyTypeSignerKeyTypeEd25519, Ed25519: testAccountID(n + 100).Ed25519}
	sponsor := testAccountID(200)
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lml),
		Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.AccountEntry{
			AccountId:     testAccountID(n),
			Balance:       xdr.Int64(balance),
			SeqNum:        xdr.SequenceNumber(int64(n) << 32),
			NumSubEntries: 2,
			Flags:         1,
			HomeDomain:    "example.org",
			Thresholds:    xdr.Thresholds{1, 2, 3, 4},
			Signers:       []xdr.Signer{{Key: signerKey, Weight: 5}},
			Ext: xdr.AccountEntryExt{V: 1, V1: &xdr.AccountEntryExtensionV1{
				Liabilities: xdr.Liabilities{Buying: 7, Selling: 8},
				Ext: xdr.AccountEntryExtensionV1Ext{V: 2, V2: &xdr.AccountEntryExtensionV2{
					NumSponsored: 1, NumSponsoring: 0,
					SignerSponsoringIDs: []xdr.SponsorshipDescriptor{&sponsor},
				}},
			}},
		}},
	}
}

func trustlineEntry(n byte, balance int64, lml uint32) xdr.LedgerEntry {
	asset := xdr.MustNewCreditAsset("USD", testAddress(9)).ToTrustLineAsset()
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lml),
		Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeTrustline, TrustLine: &xdr.TrustLineEntry{
			AccountId: testAccountID(n), Asset: asset, Balance: xdr.Int64(balance), Limit: 1_000_000, Flags: 1,
		}},
	}
}

func contractDataEntry(n byte, d xdr.ContractDataDurability, val int32, lml uint32) xdr.LedgerEntry {
	id := testContractID(n)
	sym := xdr.ScSymbol(fmt.Sprintf("key%d", n))
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lml),
		Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractData, ContractData: &xdr.ContractDataEntry{
			Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			Durability: d,
			Val:        xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: (*xdr.Int32)(&val)},
		}},
	}
}

func contractCodeEntry(n byte, lml uint32) xdr.LedgerEntry {
	code := []byte(strings.Repeat("w", 100+int(n)))
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lml),
		Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &xdr.ContractCodeEntry{
			Hash: xdr.Hash(sha256.Sum256(code)), Code: code,
		}},
	}
}

func ttlEntry(of xdr.LedgerEntry, liveUntil uint32, lml uint32) xdr.LedgerEntry {
	k, err := of.LedgerKey()
	if err != nil {
		panic(err)
	}
	b, _ := k.MarshalBinary()
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lml),
		Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeTtl, Ttl: &xdr.TtlEntry{
			KeyHash: xdr.Hash(sha256.Sum256(b)), LiveUntilLedgerSeq: xdr.Uint32(liveUntil),
		}},
	}
}

func mustKey(e xdr.LedgerEntry) xdr.LedgerKey {
	k, err := e.LedgerKey()
	if err != nil {
		panic(err)
	}
	return k
}

func ttlKeyOf(e xdr.LedgerEntry) xdr.LedgerKey {
	t := ttlEntry(e, 0, 0)
	return mustKey(t)
}

func hashOf(e xdr.LedgerEntry) []byte {
	b, _ := mustKey(e).MarshalBinary()
	s := sha256.Sum256(b)
	return s[:]
}

// Entry changes as stellar-core writes them: CREATED, STATE+UPDATED,
// STATE+REMOVED, RESTORED.
func created(e xdr.LedgerEntry) xdr.LedgerEntryChanges {
	return xdr.LedgerEntryChanges{{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: &e}}
}

func updated(pre, post xdr.LedgerEntry) xdr.LedgerEntryChanges {
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &pre},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: &post},
	}
}

func removed(pre xdr.LedgerEntry) xdr.LedgerEntryChanges {
	k := mustKey(pre)
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &pre},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &k},
	}
}

func restored(e xdr.LedgerEntry) xdr.LedgerEntryChanges {
	return xdr.LedgerEntryChanges{{Type: xdr.LedgerEntryChangeTypeLedgerEntryRestored, Restored: &e}}
}

// stateLedger is a synthetic ledger whose upgrade meta carries the given
// entry changes (the change reader yields upgrade changes like any other),
// with the given evicted keys.
type stateLedger struct {
	changes []xdr.LedgerEntryChanges
	evicted []xdr.LedgerKey
}

// stateChain builds hash-chained ledgers from start whose first previous
// hash is prev.
func stateChain(start uint32, prev xdr.Hash, ls ...stateLedger) []sequencer.Ledger {
	var out []sequencer.Ledger
	for i, l := range ls {
		seq := start + uint32(i)
		lcm := sep54test.SyntheticLCM(seq, prev, sep54test.BaseCloseTime+5*int64(seq))
		var all xdr.LedgerEntryChanges
		for _, c := range l.changes {
			all = append(all, c...)
		}
		fee := xdr.Uint32(100)
		lcm.V2.UpgradesProcessing = []xdr.UpgradeEntryMeta{{
			Upgrade: xdr.LedgerUpgrade{Type: xdr.LedgerUpgradeTypeLedgerUpgradeBaseFee, NewBaseFee: &fee},
			Changes: all,
		}}
		lcm.V2.EvictedKeys = l.evicted
		obj := sep54test.Encode(lcm)
		decoded, h, err := sep54.DecodeLedger(seq, obj.Body)
		if err != nil {
			panic(err)
		}
		out = append(out, sequencer.Ledger{Header: h, LCM: decoded, Source: sep54.SourceLake, Metadata: obj.Metadata, Size: len(obj.Body)})
		prev = h.Hash
	}
	return out
}

func processAll(t *testing.T, ls []sequencer.Ledger) []*Prepared {
	t.Helper()
	var out []*Prepared
	for _, l := range ls {
		p, err := Process(network.PublicNetworkPassphrase, l)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func describe(cs []StateChange) []string {
	var out []string
	for _, c := range cs {
		key := make([]string, len(c.key))
		for i, k := range c.key {
			if b, ok := k.([]byte); ok {
				key[i] = hex.EncodeToString(b)[:8]
			} else {
				key[i] = fmt.Sprint(k)
			}
		}
		out = append(out, c.table.name+" "+c.op.String()+" "+strings.Join(key, "|"))
	}
	return out
}

// ------------------------------------------------------------ extraction

// The real pubnet fixtures: one compacted change per key and table, the
// counts the compactor yields for the tracked types, and a deterministic
// order.
func TestStateChangesRealLedgers(t *testing.T) {
	want := map[uint32]map[string]int{
		// compacted (see the SDK's ChangeCompactor): accounts 2 created +
		// 478 updated + 1 removed, trustlines 2 + 270, contract data
		// 331 + 25 + 148, ttl 331 + 1 + 148.
		64704786: {"accounts upsert": 480, "accounts remove": 1, "trustlines upsert": 272,
			"contract_data upsert": 356, "contract_data remove": 148, "ttl upsert": 332, "ttl remove": 148},
		64704787: {"accounts upsert": 468, "trustlines upsert": 295,
			"contract_data upsert": 442, "contract_data remove": 61, "ttl upsert": 435, "ttl remove": 61},
	}
	for _, l := range sep54test.RealLedgers() {
		ledger := sequencer.Ledger{Header: mustHeader(t, l.Seq, l.LCM), LCM: l.LCM, Source: sep54.SourceAWS}
		p1, err := Process(network.PublicNetworkPassphrase, ledger)
		if err != nil {
			t.Fatal(err)
		}
		p2, err := Process(network.PublicNetworkPassphrase, ledger)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]int{}
		seen := map[string]bool{}
		for _, c := range p1.State {
			got[c.table.name+" "+c.op.String()]++
			id := c.table.name + "\x00" + c.id
			if seen[id] {
				t.Fatalf("ledger %d: key %q appears twice", l.Seq, id)
			}
			seen[id] = true
			if c.Ledger != l.Seq {
				t.Fatalf("change ledger %d", c.Ledger)
			}
			if c.op == opUpsert && len(c.vals) != len(c.table.cols) {
				t.Fatalf("%s row has %d values, %d columns", c.table.name, len(c.vals), len(c.table.cols))
			}
		}
		for k, v := range want[l.Seq] {
			if got[k] != v {
				t.Errorf("ledger %d: %s = %d, want %d (all %v)", l.Seq, k, got[k], v, got)
			}
		}
		if !slices.Equal(describe(p1.State), describe(p2.State)) {
			t.Fatalf("ledger %d: state changes are not deterministic", l.Seq)
		}
		h, err := HistoryProcessor(network.PublicNetworkPassphrase)(ledger)
		if err != nil || h.State != nil {
			t.Fatalf("history processor: state %d, err %v", len(h.State), err)
		}
	}
}

func mustHeader(t *testing.T, seq uint32, lcm xdr.LedgerCloseMeta) sep54.Header {
	t.Helper()
	h, err := sep54.CheckLCM(seq, lcm)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Every change type and eviction rule of the state tables, read from a
// ledger's meta; the rows carry the decoded columns.
func TestStateChangesSynthetic(t *testing.T) {
	acctB := accountEntry(2, 200, 10)
	tl := trustlineEntry(1, 50, 10)
	persistent := contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 7, 10)
	temporary := contractDataEntry(2, xdr.ContractDataDurabilityTemporary, 8, 10)
	code := contractCodeEntry(1, 10)
	restoredData := contractDataEntry(3, xdr.ContractDataDurabilityPersistent, 9, 20)

	ls := stateChain(20, xdr.Hash{1}, stateLedger{
		changes: []xdr.LedgerEntryChanges{
			created(accountEntry(1, 100, 20)),
			updated(tl, trustlineEntry(1, 75, 20)),
			removed(acctB),
			restored(restoredData),
			restored(ttlEntry(restoredData, 500, 20)),
			created(contractCodeEntry(2, 20)),
		},
		evicted: []xdr.LedgerKey{mustKey(persistent), ttlKeyOf(persistent), mustKey(temporary), ttlKeyOf(temporary), mustKey(code)},
	})
	p := processAll(t, ls)[0]
	got := describe(p.State)
	want := []string{
		"accounts upsert " + testAddress(1),
		"accounts remove " + testAddress(2),
		"trustlines upsert " + testAddress(1) + "|USD:" + testAddress(9),
		"contract_data upsert " + hex.EncodeToString(hashOf(restoredData))[:8],
		"contract_code upsert " + hex.EncodeToString(hashOf(contractCodeEntry(2, 0)))[:8],
		"ttl upsert " + hex.EncodeToString(hashOf(restoredData))[:8],
		// evictions follow the ledger's changes
		"contract_data evict_archive " + hex.EncodeToString(hashOf(persistent))[:8],
		"contract_data evict_delete " + hex.EncodeToString(hashOf(temporary))[:8],
		"contract_code evict_archive " + hex.EncodeToString(hashOf(code))[:8],
		"ttl evict_delete " + hex.EncodeToString(hashOf(persistent))[:8],
		"ttl evict_delete " + hex.EncodeToString(hashOf(temporary))[:8],
	}
	// Within a group, order is by key; compare as sets per group.
	if !sameGroups(got[:6], want[:6]) || !sameGroups(got[6:], want[6:]) {
		t.Fatalf("state changes\n got %q\nwant %q", got, want)
	}

	// Decoded columns.
	for _, c := range p.State {
		if c.op != opUpsert {
			continue
		}
		switch c.table {
		case &accountsTable:
			row := c.vals
			if row[0] != testAddress(1) || row[1] != int64(100) || row[10] != "example.org" || row[11] != int16(1) ||
				row[14] != int16(4) || row[16] != int64(7) || row[17] != int64(8) || row[19] != int64(20) {
				t.Fatalf("account row %v", row)
			}
			wantSigners := fmt.Sprintf(`[{"key":%q,"weight":5,"sponsor":%q}]`, testAddressSigner(101), testAddress(200))
			if row[15] != wantSigners {
				t.Fatalf("signers %s, want %s", row[15], wantSigners)
			}
		case &trustlinesTable:
			if c.vals[3] != int64(75) || c.vals[2] != nil {
				t.Fatalf("trustline row %v", c.vals)
			}
		case &contractDataTable:
			cid, _ := strkey.Encode(strkey.VersionByteContract, func() []byte { id := testContractID(3); return id[:] }())
			if c.vals[1] != cid || c.vals[2] != "persistent" || c.vals[5] != `{"symbol":"key3"}` || c.vals[9] != nil || c.vals[12] != false {
				t.Fatalf("contract data row %v", c.vals)
			}
		case &contractCodeTable:
			if c.vals[2] != int32(102) {
				t.Fatalf("contract code row %v", c.vals)
			}
		case &ttlTable:
			if c.vals[1] != int64(500) {
				t.Fatalf("ttl row %v", c.vals)
			}
		}
	}
}

func testAddressSigner(n byte) string {
	a := testAccountID(n)
	s, _ := a.GetAddress()
	return s
}

func sameGroups(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// A SAC balance entry gets balance_holder and balance.
func TestSACBalanceColumns(t *testing.T) {
	asset := xdr.MustNewCreditAsset("USD", testAddress(9))
	assetID, err := asset.ContractID(network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	holder := testContractID(42)
	data := sac.BalanceToContractData(assetID, holder, 12345)
	e := xdr.LedgerEntry{LastModifiedLedgerSeq: 5, Data: data}
	vals, err := contractDataValues(e, network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	wantHolder, _ := strkey.Encode(strkey.VersionByteContract, holder[:])
	if vals[7] != wantHolder || vals[8] != big.NewInt(12345).String() {
		t.Fatalf("holder %v balance %v", vals[7], vals[8])
	}
}

// ------------------------------------------------------------ merge

func TestMergeState(t *testing.T) {
	data := contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 1, 10)
	temp := contractDataEntry(2, xdr.ContractDataDurabilityTemporary, 1, 10)
	up := func(e xdr.LedgerEntry, ledger uint32) StateChange {
		c, ok, err := upsertChange(e, ledger, network.PublicNetworkPassphrase)
		if err != nil || !ok {
			t.Fatal(err)
		}
		return c
	}
	key := func(e xdr.LedgerEntry, ledger uint32, evicted bool) StateChange {
		c, ok, err := keyChange(mustKey(e), ledger, evicted)
		if err != nil || !ok {
			t.Fatal(err)
		}
		return c
	}
	for _, tc := range []struct {
		name    string
		ledgers [][]StateChange
		want    string // op of the one merged change
		check   func(StateChange) bool
	}{
		{"upsert then upsert keeps the last", [][]StateChange{{up(data, 1)}, {up(contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 2, 11), 2)}},
			"upsert", func(c StateChange) bool { return c.vals[13] == int64(11) }},
		{"upsert then remove", [][]StateChange{{up(data, 1)}, {key(data, 2, false)}}, "remove", nil},
		{"remove then upsert", [][]StateChange{{key(data, 1, false)}, {up(data, 2)}}, "upsert", nil},
		{"evict archive then restore", [][]StateChange{{key(data, 1, true)}, {up(data, 2)}},
			"upsert", func(c StateChange) bool { return c.vals[12] == false }},
		{"upsert then evict archive keeps the row, archived", [][]StateChange{{up(data, 1)}, {key(data, 2, true)}},
			"upsert", func(c StateChange) bool { return c.vals[12] == true && c.vals[13] == int64(10) }},
		{"upsert then evict temporary removes without expectation", [][]StateChange{{up(temp, 1)}, {key(temp, 2, true)}}, "remove", nil},
		{"remove then evict keeps the expectation (drift)", [][]StateChange{{key(temp, 1, false)}, {key(temp, 2, true)}}, "evict_delete", nil},
		{"evict alone", [][]StateChange{{key(data, 1, true)}}, "evict_archive", nil},
		{"same ledger: upsert then eviction", [][]StateChange{{up(data, 1), key(data, 1, true)}}, "upsert",
			func(c StateChange) bool { return c.vals[12] == true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := describe(tc.ledgers[0])
			got := mergeState(tc.ledgers)
			if len(got) != 1 || got[0].op.String() != tc.want {
				t.Fatalf("merged %q, want one %s", describe(got), tc.want)
			}
			if tc.check != nil && !tc.check(got[0]) {
				t.Fatalf("merged row %v", got[0].vals)
			}
			if !slices.Equal(before, describe(tc.ledgers[0])) {
				t.Fatal("merge modified its input")
			}
		})
	}
	// Different keys and tables stay separate, in first-seen order.
	got := mergeState([][]StateChange{{up(data, 1), up(temp, 1)}, {up(accountEntry(1, 1, 2), 2), up(data, 2)}})
	if len(got) != 3 {
		t.Fatalf("merged %q", describe(got))
	}
	// Archiving must not touch the source row of an earlier upsert.
	src := up(data, 1)
	_ = mergeState([][]StateChange{{src}, {key(data, 2, true)}})
	if src.vals[12] != false {
		t.Fatal("merge modified the upsert's values")
	}
}

// ------------------------------------------------------------ database

func stateCount(t *testing.T, conn *pgx.Conn, table, where string) int64 {
	t.Helper()
	q := "SELECT count(*) FROM explorer." + table
	if where != "" {
		q += " WHERE " + where
	}
	return count(t, conn, q)
}

func seedStateCheckpoint(t *testing.T, db interface{ Exec(...string) }, seq uint32) {
	t.Helper()
	db.Exec(fmt.Sprintf(`INSERT INTO explorer.state_checkpoint (checkpoint, hash, load_seconds, entries) VALUES (%d, decode(repeat('00', 32), 'hex'), 0, '{}')`, seq))
}

// The sink applies state once the state was loaded: upserts
// guarded by last_modified_ledger, removals, evictions (delete or archive),
// drift alerts, and batch merging.
func TestSinkAppliesState(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	conn := db.Conn()

	// The checkpoint state at ledger 99.
	acctB := accountEntry(2, 200, 50)
	tl := trustlineEntry(1, 50, 50)
	persistent := contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 7, 50)
	temporary := contractDataEntry(2, xdr.ContractDataDurabilityTemporary, 8, 50)
	code := contractCodeEntry(1, 50)
	newer := accountEntry(5, 555, 150) // already newer than what ledger 100 writes
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loader := NewStateLoader(tx, network.PublicNetworkPassphrase)
	for _, e := range []xdr.LedgerEntry{acctB, tl, persistent, temporary, code, newer,
		ttlEntry(persistent, 1000, 50), ttlEntry(temporary, 1000, 50), offerEntry()} {
		if err := loader.Add(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := loader.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if c := loader.Counts(); c["accounts"] != 2 || c["trustlines"] != 1 || c["contract_data"] != 2 || c["contract_code"] != 1 || c["ttl"] != 2 ||
		c["offers"] != 1 {
		t.Fatalf("loaded %v", c)
	}

	prev := xdr.Hash{9}
	seedCursor(t, db, Pipeline, 99, prev)
	seedStateCheckpoint(t, db, 99)
	al := &alerts{}
	sink := newSink(t, db.WriterDSN, SinkOptions{Alert: al.alert})
	if err := sink.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !sink.StateLoaded() {
		t.Fatal("state not loaded")
	}

	missing := contractDataEntry(9, xdr.ContractDataDurabilityPersistent, 1, 50) // never loaded
	ls := stateChain(100, prev,
		// ledger 100: one ledger per commit
		stateLedger{
			changes: []xdr.LedgerEntryChanges{
				created(accountEntry(1, 100, 100)),
				updated(tl, trustlineEntry(1, 75, 100)),
				removed(acctB),
				updated(newer, accountEntry(5, 1, 100)), // older than the row: ignored
			},
			evicted: []xdr.LedgerKey{mustKey(persistent), ttlKeyOf(persistent), mustKey(temporary), ttlKeyOf(temporary),
				mustKey(code), mustKey(missing)},
		},
		// ledgers 101-102 in one batch: restore, then update and evict.
		stateLedger{changes: []xdr.LedgerEntryChanges{
			restored(contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 70, 101)),
			created(contractDataEntry(3, xdr.ContractDataDurabilityTemporary, 3, 101)),
		}},
		stateLedger{
			changes: []xdr.LedgerEntryChanges{updated(contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 70, 101),
				contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 71, 102))},
			evicted: []xdr.LedgerKey{mustKey(persistent), mustKey(contractDataEntry(3, xdr.ContractDataDurabilityTemporary, 3, 0))},
		},
	)
	ps := processAll(t, ls)
	if err := sink.Commit(ctx, ps[:1]); err != nil {
		t.Fatal(err)
	}
	if got := stateCount(t, conn, "accounts", fmt.Sprintf("account_id = '%s' AND balance = 100 AND last_modified_ledger = 100", testAddress(1))); got != 1 {
		t.Fatal("created account missing")
	}
	if stateCount(t, conn, "accounts", fmt.Sprintf("account_id = '%s'", testAddress(2))) != 0 {
		t.Fatal("removed account still present")
	}
	if stateCount(t, conn, "accounts", fmt.Sprintf("account_id = '%s' AND balance = 555", testAddress(5))) != 1 {
		t.Fatal("an older update overwrote a newer row")
	}
	if stateCount(t, conn, "trustlines", "balance = 75 AND last_modified_ledger = 100") != 1 {
		t.Fatal("trustline not updated")
	}
	if stateCount(t, conn, "contract_data", fmt.Sprintf(`key_hash = '\x%x' AND archived`, hashOf(persistent))) != 1 {
		t.Fatal("persistent data not archived")
	}
	if stateCount(t, conn, "contract_data", fmt.Sprintf(`key_hash = '\x%x'`, hashOf(temporary))) != 0 {
		t.Fatal("temporary data not deleted")
	}
	if stateCount(t, conn, "contract_code", "archived") != 1 || stateCount(t, conn, "ttl", "") != 0 {
		t.Fatal("code not archived or ttl rows left")
	}
	if got := al.list(); !slices.Equal(got, []string{"state_drift"}) || sink.StateDrift() != 1 {
		t.Fatalf("alerts %v, drift %d: want one drift for the never-loaded entry", got, sink.StateDrift())
	}

	// The batch 101-102: restored then updated then evicted → the updated
	// row, archived; the temporary entry created then evicted is gone
	// without a drift alert.
	if err := sink.Commit(ctx, ps[1:]); err != nil {
		t.Fatal(err)
	}
	if stateCount(t, conn, "contract_data", fmt.Sprintf(`key_hash = '\x%x' AND archived AND last_modified_ledger = 102 AND val = '{"i32":71}'`, hashOf(persistent))) != 1 {
		var row string
		_ = conn.QueryRow(ctx, "SELECT val::text || ' ' || archived || ' ' || last_modified_ledger FROM explorer.contract_data WHERE key_hash = $1", hashOf(persistent)).Scan(&row)
		t.Fatalf("persistent entry after the batch: %s", row)
	}
	if stateCount(t, conn, "contract_data", "durability = 'temporary'") != 0 {
		t.Fatal("temporary entry left")
	}
	if len(al.list()) != 1 {
		t.Fatalf("alerts %v", al.list())
	}

	// A reindex sink never touches state.
	seedCursor(t, db, "reindex:100-100", 99, prev)
	reindex := newSink(t, db.WriterDSN, SinkOptions{Pipeline: "reindex:100-100", Replace: true})
	if err := reindex.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if reindex.StateLoaded() {
		t.Fatal("a reindex sink maintains state")
	}
}

func offerEntry() xdr.LedgerEntry {
	return offerOf(0, 0, xdr.Price{N: 1, D: 1}, 0)
}

// offerOf is offer id of seller 3: selling XLM for USD at price, amount
// stroops.
func offerOf(id int64, amount int64, price xdr.Price, lml uint32) xdr.LedgerEntry {
	return xdr.LedgerEntry{LastModifiedLedgerSeq: xdr.Uint32(lml), Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeOffer, Offer: &xdr.OfferEntry{
		SellerId: testAccountID(3), OfferId: xdr.Int64(id), Selling: xdr.MustNewNativeAsset(),
		Buying: xdr.MustNewCreditAsset("USD", testAddress(9)), Amount: xdr.Int64(amount), Price: price, Flags: 1,
	}}}
}

func dataEntryOf(n byte, name, value string, lml uint32) xdr.LedgerEntry {
	return xdr.LedgerEntry{LastModifiedLedgerSeq: xdr.Uint32(lml), Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeData, Data: &xdr.DataEntry{
		AccountId: testAccountID(n), DataName: xdr.String64(name), DataValue: xdr.DataValue(value),
	}}}
}

func claimableBalanceOf(n byte, lml uint32) xdr.LedgerEntry {
	before := xdr.Int64(1700000000)
	return xdr.LedgerEntry{LastModifiedLedgerSeq: xdr.Uint32(lml), Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeClaimableBalance,
		ClaimableBalance: &xdr.ClaimableBalanceEntry{
			BalanceId: xdr.ClaimableBalanceId{Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0, V0: &xdr.Hash{n}},
			Asset:     xdr.MustNewCreditAsset("USD", testAddress(9)), Amount: 25,
			Claimants: []xdr.Claimant{
				{Type: xdr.ClaimantTypeClaimantTypeV0, V0: &xdr.ClaimantV0{Destination: testAccountID(7),
					Predicate: xdr.ClaimPredicate{Type: xdr.ClaimPredicateTypeClaimPredicateUnconditional}}},
				{Type: xdr.ClaimantTypeClaimantTypeV0, V0: &xdr.ClaimantV0{Destination: testAccountID(8),
					Predicate: xdr.ClaimPredicate{Type: xdr.ClaimPredicateTypeClaimPredicateBeforeAbsoluteTime, AbsBefore: &before}}},
			},
		}}}
}

func liquidityPoolOf(n byte, reserveA int64, lml uint32) xdr.LedgerEntry {
	return xdr.LedgerEntry{LastModifiedLedgerSeq: xdr.Uint32(lml), Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeLiquidityPool,
		LiquidityPool: &xdr.LiquidityPoolEntry{LiquidityPoolId: xdr.PoolId{n}, Body: xdr.LiquidityPoolEntryBody{
			Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct, ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
				Params: xdr.LiquidityPoolConstantProductParameters{AssetA: xdr.MustNewNativeAsset(),
					AssetB: xdr.MustNewCreditAsset("USD", testAddress(9)), Fee: 30},
				ReserveA: xdr.Int64(reserveA), ReserveB: 20, TotalPoolShares: 100, PoolSharesTrustLineCount: 2,
			}}}}}
}

// The DEX and account tables, contract instances and Wasm: loaded from a
// checkpoint, then maintained per ledger.
func TestSinkAppliesNewStateTables(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	conn := db.Conn()
	usd := xdr.MustNewCreditAsset("USD", testAddress(9))
	spec := xdr.ScSpecEntry{Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0, FunctionV0: &xdr.ScSpecFunctionV0{Name: "hello"}}
	wasm := wasmModule(customSectionOf(specSection, specPayload(t, spec)))
	code := xdr.LedgerEntry{LastModifiedLedgerSeq: 50, Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.ContractCodeEntry{Hash: xdr.Hash(sha256.Sum256(wasm)), Code: wasm}}}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loader := NewStateLoader(tx, network.PublicNetworkPassphrase)
	for _, e := range []xdr.LedgerEntry{offerOf(7, 100, xdr.Price{N: 3, D: 2}, 50), dataEntryOf(1, "k", "v1", 50), claimableBalanceOf(1, 50),
		liquidityPoolOf(5, 1000, 50), code, instanceEntry(t, 2, nil), instanceEntry(t, 0, &usd)} {
		if err := loader.Add(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := loader.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if c := loader.Counts(); c["offers"] != 1 || c["data_entries"] != 1 || c["claimable_balances"] != 1 || c["liquidity_pools"] != 1 ||
		c["contract_code"] != 1 || c["contract_data"] != 2 {
		t.Fatalf("loaded %v", c)
	}
	for what, where := range map[string]string{
		"offer price":          "offers:offer_id = 7 AND price = 1.5 AND selling = 'native' AND flags = 1",
		"claimant lookup":      fmt.Sprintf(`claimable_balances:claimants @> '[{"destination": "%s"}]' AND asset = 'USD:%s'`, testAddress(8), testAddress(9)),
		"claimant predicate":   `claimable_balances:claimants->1->'predicate' = '{"abs_before": "1700000000"}'`,
		"data value":           fmt.Sprintf(`data_entries:account_id = '%s' AND name = 'k' AND value = 'v1'::bytea`, testAddress(1)),
		"pool":                 "liquidity_pools:asset_a = 'native' AND fee_bp = 30 AND reserve_a = 1000 AND trustlines = 2",
		"wasm and spec":        `contract_code:wasm_size = length(wasm) AND spec->0->'function_v0'->>'name' = 'hello'`,
		"wasm instance":        "contract_data:executable = 'wasm' AND length(wasm_hash) = 32 AND asset IS NULL",
		"stellar asset":        fmt.Sprintf("contract_data:executable = 'stellar_asset' AND asset = 'USD:%s'", testAddress(9)),
		"balance id as strkey": "claimable_balances:balance_id LIKE 'B%'",
	} {
		table, cond, _ := strings.Cut(where, ":")
		if stateCount(t, conn, table, cond) != 1 {
			t.Errorf("%s: no %s row with %s", what, table, cond)
		}
	}

	prev := xdr.Hash{9}
	seedCursor(t, db, Pipeline, 99, prev)
	seedStateCheckpoint(t, db, 99)
	al := &alerts{}
	sink := newSink(t, db.WriterDSN, SinkOptions{Alert: al.alert})
	if err := sink.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ps := processAll(t, stateChain(100, prev, stateLedger{changes: []xdr.LedgerEntryChanges{
		updated(offerOf(7, 100, xdr.Price{N: 3, D: 2}, 50), offerOf(7, 60, xdr.Price{N: 1, D: 4}, 100)),
		created(offerOf(8, 10, xdr.Price{N: 2, D: 1}, 100)),
		removed(dataEntryOf(1, "k", "v1", 50)),
		created(dataEntryOf(1, "k2", "v2", 100)),
		removed(claimableBalanceOf(1, 50)),
		updated(liquidityPoolOf(5, 1000, 50), liquidityPoolOf(5, 900, 100)),
	}}))
	if err := sink.Commit(ctx, ps); err != nil {
		t.Fatal(err)
	}
	for what, where := range map[string]string{
		"updated offer": "offers:offer_id = 7 AND amount = 60 AND price = 0.25 AND last_modified_ledger = 100",
		"new offer":     "offers:offer_id = 8 AND price = 2",
		"new data":      "data_entries:name = 'k2' AND value = 'v2'::bytea",
		"updated pool":  "liquidity_pools:reserve_a = 900",
	} {
		table, cond, _ := strings.Cut(where, ":")
		if stateCount(t, conn, table, cond) != 1 {
			t.Errorf("%s: no %s row with %s", what, table, cond)
		}
	}
	if stateCount(t, conn, "data_entries", "name = 'k'") != 0 || stateCount(t, conn, "claimable_balances", "") != 0 {
		t.Error("removed data entry or claimed balance left")
	}
	if len(al.list()) != 0 {
		t.Fatalf("alerts %v", al.list())
	}
}

// After `bootstrap --history-only` (no state checkpoint) the state tables
// are left alone.
func TestSinkHistoryOnlySkipsState(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	prev := xdr.Hash{9}
	seedCursor(t, db, Pipeline, 99, prev)
	sink := newSink(t, db.WriterDSN, SinkOptions{})
	if err := sink.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.StateLoaded() {
		t.Fatal("state loaded without a checkpoint")
	}
	ps := processAll(t, stateChain(100, prev, stateLedger{changes: []xdr.LedgerEntryChanges{created(accountEntry(1, 1, 100))}}))
	if err := sink.Commit(ctx, ps); err != nil {
		t.Fatal(err)
	}
	if n := stateCount(t, db.Conn(), "accounts", ""); n != 0 {
		t.Fatalf("%d accounts written", n)
	}
}

func TestStateCheckpointRoundTrip(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	conn := db.Conn()
	if _, ok, err := ReadStateCheckpoint(ctx, conn); err != nil || ok {
		t.Fatalf("empty: ok %v err %v", ok, err)
	}
	for _, seq := range []uint32{127, 191} {
		sc := StateCheckpoint{Checkpoint: seq, Hash: xdr.Hash{byte(seq)}, LoadSeconds: 1.5, Entries: map[string]int64{"accounts": int64(seq)}}
		if err := WriteStateCheckpoint(ctx, conn, sc); err != nil {
			t.Fatal(err)
		}
		got, ok, err := ReadStateCheckpoint(ctx, conn)
		if err != nil || !ok || got.Checkpoint != seq || got.Hash != sc.Hash || got.Entries["accounts"] != int64(seq) {
			t.Fatalf("read %+v %v %v", got, ok, err)
		}
	}
	if n := count(t, conn, "SELECT count(*) FROM explorer.state_checkpoint"); n != 1 {
		t.Fatalf("%d rows", n)
	}
}

// stateChecksums hashes every state table's ordered content.
func stateChecksums(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, tbl := range StateTableNames() {
		var sum string
		var n int64
		q := fmt.Sprintf("SELECT md5(coalesce(string_agg(r::text, '|' ORDER BY r::text), '')), count(*) FROM explorer.%s r", tbl)
		if err := conn.QueryRow(context.Background(), q).Scan(&sum, &n); err != nil {
			t.Fatal(err)
		}
		out[tbl] = fmt.Sprintf("%d/%s", n, sum)
	}
	return out
}

// Merging a batch last-op-wins gives exactly the state of applying its
// ledgers one by one: real pubnet content (re-chained), committed one
// ledger per transaction in one database and in batches of 5 (COPY path)
// in another.
func TestBatchedStateEqualsPerLedger(t *testing.T) {
	objs := sep54test.Rechain(66_000_000, 15)
	ps := prepare(t, objs, sep54.SourceLake)
	first, _, err := sep54.DecodeLedger(66_000_000, objs[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	prev := first.LedgerHeaderHistoryEntry().Header.PreviousLedgerHash
	var sums []map[string]string
	for _, batch := range []int{1, 5} {
		db := migrated(t)
		seedCursor(t, db, Pipeline, 66_000_000-1, prev)
		seedStateCheckpoint(t, db, 66_000_000-1)
		al := &alerts{}
		sink := newSink(t, db.WriterDSN, SinkOptions{Alert: al.alert})
		if err := sink.Start(context.Background()); err != nil || !sink.StateLoaded() {
			t.Fatalf("start: %v", err)
		}
		for i := 0; i < len(ps); i += batch {
			if err := sink.Commit(context.Background(), ps[i:min(i+batch, len(ps))]); err != nil {
				t.Fatal(err)
			}
		}
		if len(al.list()) != 0 {
			t.Fatalf("batch %d: alerts %v", batch, al.list())
		}
		sums = append(sums, stateChecksums(t, db.Conn()))
	}
	if fmt.Sprint(sums[0]) != fmt.Sprint(sums[1]) {
		t.Fatalf("per-ledger %v\nbatched    %v", sums[0], sums[1])
	}
	if strings.HasPrefix(sums[0]["accounts"], "0/") || strings.HasPrefix(sums[0]["contract_data"], "0/") {
		t.Fatalf("no state written: %v", sums[0])
	}
	t.Logf("state after 15 ledgers: %v", sums[0])
}
