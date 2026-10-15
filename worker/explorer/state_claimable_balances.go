package explorer

import (
	"fmt"
	"reflect"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// claimable_balances: balances waiting for a claimant
// (state_claimable_balances.sql).

var claimableBalancesTable = stateTable{table: table{
	name: "claimable_balances",
	key:  []string{"balance_id"},
	cols: []column{
		{"balance_id", "text"}, {"asset", "text"}, {"amount", "bigint"}, {"claimants", "jsonb"},
		{"flags", "integer"}, {"sponsor", "text"}, {"last_modified_ledger", "bigint"},
	},
}}

func claimableBalanceValues(e xdr.LedgerEntry) ([]any, error) {
	cb := e.Data.MustClaimableBalance()
	id, err := cb.BalanceId.EncodeToStrkey()
	if err != nil {
		return nil, err
	}
	asset, err := assetName(cb.Asset)
	if err != nil {
		return nil, fmt.Errorf("claimable balance %s: %w", id, err)
	}
	claimants, err := claimantsJSON(cb.Claimants)
	if err != nil {
		return nil, fmt.Errorf("claimable balance %s claimants: %w", id, err)
	}
	sponsor, err := sponsorOf(e)
	if err != nil {
		return nil, err
	}
	return []any{id, asset, int64(cb.Amount), claimants, int32(cb.Flags()), sponsor, int64(e.LastModifiedLedgerSeq)}, nil
}

// claimantsJSON encodes the claimants as [{"destination", "predicate"}],
// the predicate as the XDR's JSON.
func claimantsJSON(cs []xdr.Claimant) (string, error) {
	dst := []byte{'['}
	for i, c := range cs {
		v0, ok := c.GetV0()
		if !ok {
			return "", fmt.Errorf("claimant %d: unknown type %d", i, c.Type)
		}
		dest, err := v0.Destination.GetAddress()
		if err != nil {
			return "", err
		}
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"destination":`...)
		dst = appendJSONString(dst, dest)
		dst = append(dst, `,"predicate":`...)
		if dst, err = appendXDR(dst, reflect.ValueOf(v0.Predicate)); err != nil {
			return "", err
		}
		dst = append(dst, '}')
	}
	return string(append(dst, ']')), nil
}
