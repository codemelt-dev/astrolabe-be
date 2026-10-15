package explorer

import "github.com/stellar/go-stellar-sdk/xdr"

// data_entries: each account's ManageData pairs (state_data_entries.sql).

var dataEntriesTable = stateTable{table: table{
	name: "data_entries",
	key:  []string{"account_id", "name"},
	cols: []column{
		{"account_id", "text"}, {"name", "text"}, {"value", "bytea"}, {"sponsor", "text"},
		{"last_modified_ledger", "bigint"},
	},
}}

// dataKey is the identity of a data entry: account and name.
func dataKey(k xdr.LedgerKeyData) ([]any, error) {
	id, err := k.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}
	return []any{id, cleanText(string(k.DataName))}, nil
}

func dataValues(e xdr.LedgerEntry) ([]any, error) {
	d := e.Data.MustData()
	id, err := d.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}
	sponsor, err := sponsorOf(e)
	if err != nil {
		return nil, err
	}
	return []any{id, cleanText(string(d.DataName)), []byte(d.DataValue), sponsor, int64(e.LastModifiedLedgerSeq)}, nil
}
