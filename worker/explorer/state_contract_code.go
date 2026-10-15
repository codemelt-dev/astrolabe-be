package explorer

import "github.com/stellar/go-stellar-sdk/xdr"

// contract_code: uploaded Wasm with its contractspecv0 section as JSON
// (state_contract_code.sql, wasmspec.go).

var contractCodeTable = stateTable{archivable: true, table: table{
	name: "contract_code",
	key:  []string{"key_hash"},
	cols: []column{
		{"key_hash", "bytea"}, {"code_hash", "bytea"}, {"wasm_size", "integer"}, {"wasm", "bytea"},
		{"spec", "jsonb"}, {"archived", "boolean"}, {"last_modified_ledger", "bigint"},
	},
}}

func contractCodeValues(e xdr.LedgerEntry) ([]any, error) {
	cc := e.Data.MustContractCode()
	key, err := e.LedgerKey()
	if err != nil {
		return nil, err
	}
	kh, err := keyHash(key)
	if err != nil {
		return nil, err
	}
	return []any{kh, cc.Hash[:], int32(len(cc.Code)), []byte(cc.Code), wasmSpec(cc.Code), false, int64(e.LastModifiedLedgerSeq)}, nil
}
