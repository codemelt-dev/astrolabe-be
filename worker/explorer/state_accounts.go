package explorer

import (
	"fmt"
	"strconv"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// accounts: current state of every account (state_accounts.sql).

var accountsTable = stateTable{table: table{
	name: "accounts",
	key:  []string{"account_id"},
	cols: []column{
		{"account_id", "text"}, {"balance", "bigint"}, {"seq_num", "bigint"}, {"seq_ledger", "bigint"},
		{"seq_time", "bigint"}, {"num_subentries", "integer"}, {"num_sponsored", "integer"},
		{"num_sponsoring", "integer"}, {"inflation_dest", "text"}, {"flags", "integer"},
		{"home_domain", "text"}, {"master_weight", "smallint"}, {"threshold_low", "smallint"},
		{"threshold_medium", "smallint"}, {"threshold_high", "smallint"}, {"signers", "jsonb"},
		{"buying_liabilities", "bigint"}, {"selling_liabilities", "bigint"}, {"sponsor", "text"},
		{"last_modified_ledger", "bigint"},
	},
}}

func accountKey(k xdr.LedgerKeyAccount) (string, error) {
	return k.AccountId.GetAddress()
}

// accountValues returns the accounts row of an account entry.
func accountValues(e xdr.LedgerEntry) ([]any, error) {
	a := e.Data.MustAccount()
	id, err := a.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}
	var seqLedger, seqTime any
	if v3, ok := accountExtV3(a); ok {
		seqLedger, seqTime = int64(v3.SeqLedger), int64(v3.SeqTime)
	}
	var inflation any
	if a.InflationDest != nil {
		s, err := a.InflationDest.GetAddress()
		if err != nil {
			return nil, err
		}
		inflation = s
	}
	signers, err := signersJSON(a)
	if err != nil {
		return nil, fmt.Errorf("account %s signers: %w", id, err)
	}
	sponsor, err := sponsorOf(e)
	if err != nil {
		return nil, err
	}
	liab := a.Liabilities()
	return []any{
		id, int64(a.Balance), int64(a.SeqNum), seqLedger,
		seqTime, int32(a.NumSubEntries), int32(a.NumSponsored()),
		int32(a.NumSponsoring()), inflation, int32(a.Flags),
		cleanText(string(a.HomeDomain)), int16(a.MasterKeyWeight()), int16(a.ThresholdLow()),
		int16(a.ThresholdMedium()), int16(a.ThresholdHigh()), signers,
		int64(liab.Buying), int64(liab.Selling), sponsor,
		int64(e.LastModifiedLedgerSeq),
	}, nil
}

// accountExtV3 returns the AccountEntryExtensionV3 (seq_ledger, seq_time),
// present on accounts that bumped their sequence since protocol 19.
func accountExtV3(a xdr.AccountEntry) (xdr.AccountEntryExtensionV3, bool) {
	if a.Ext.V1 == nil || a.Ext.V1.Ext.V2 == nil || a.Ext.V1.Ext.V2.Ext.V3 == nil {
		return xdr.AccountEntryExtensionV3{}, false
	}
	return *a.Ext.V1.Ext.V2.Ext.V3, true
}

// signersJSON encodes the signers as [{"key", "weight", "sponsor"}], in the
// entry's order (which is core's canonical order).
func signersJSON(a xdr.AccountEntry) (string, error) {
	sponsors := a.SignerSponsoringIDs()
	dst := []byte{'['}
	for i, s := range a.Signers {
		if i > 0 {
			dst = append(dst, ',')
		}
		key, err := s.Key.GetAddress()
		if err != nil {
			return "", err
		}
		dst = append(dst, `{"key":`...)
		dst = appendJSONString(dst, key)
		dst = append(dst, `,"weight":`...)
		dst = strconv.AppendUint(dst, uint64(s.Weight), 10)
		dst = append(dst, `,"sponsor":`...)
		if i < len(sponsors) && sponsors[i] != nil {
			sp, err := sponsors[i].GetAddress()
			if err != nil {
				return "", err
			}
			dst = appendJSONString(dst, sp)
		} else {
			dst = append(dst, "null"...)
		}
		dst = append(dst, '}')
	}
	return string(append(dst, ']')), nil
}

// sponsorOf returns the G… sponsor of an entry, or nil.
func sponsorOf(e xdr.LedgerEntry) (any, error) {
	sp := e.SponsoringID()
	if sp == nil {
		return nil, nil
	}
	return sp.GetAddress()
}
