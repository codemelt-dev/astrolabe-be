package explorer

import "github.com/stellar/go-stellar-sdk/xdr"

// ttl: live-until ledger of every contract data and code entry
// (state_ttl.sql). key_hash is TtlEntry.KeyHash, i.e. the key_hash of the
// contract_data or contract_code row it belongs to.

var ttlTable = stateTable{table: table{
	name: "ttl",
	key:  []string{"key_hash"},
	cols: []column{{"key_hash", "bytea"}, {"live_until_ledger", "bigint"}, {"last_modified_ledger", "bigint"}},
}}

func ttlValues(e xdr.LedgerEntry) ([]any, error) {
	t := e.Data.MustTtl()
	return []any{t.KeyHash[:], int64(t.LiveUntilLedgerSeq), int64(e.LastModifiedLedgerSeq)}, nil
}
