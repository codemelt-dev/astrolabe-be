package explorer

import (
	"encoding/hex"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// trustlines: current state of every trustline (state_trustlines.sql).

var trustlinesTable = stateTable{table: table{
	name: "trustlines",
	key:  []string{"account_id", "asset"},
	cols: []column{
		{"account_id", "text"}, {"asset", "text"}, {"liquidity_pool_id", "bytea"}, {"balance", "bigint"},
		{"trust_limit", "bigint"}, {"flags", "integer"}, {"buying_liabilities", "bigint"},
		{"selling_liabilities", "bigint"}, {"sponsor", "text"}, {"last_modified_ledger", "bigint"},
	},
}}

// trustlineAsset returns the asset column ('CODE:ISSUER', or the pool id in
// hex for pool shares) and the pool id.
func trustlineAsset(a xdr.TrustLineAsset) (string, []byte, error) {
	switch a.Type {
	case xdr.AssetTypeAssetTypePoolShare:
		id := a.MustLiquidityPoolId()
		return hex.EncodeToString(id[:]), id[:], nil
	case xdr.AssetTypeAssetTypeCreditAlphanum4, xdr.AssetTypeAssetTypeCreditAlphanum12:
		var typ, code, issuer string
		if err := a.Extract(&typ, &code, &issuer); err != nil {
			return "", nil, err
		}
		return cleanText(code) + ":" + issuer, nil, nil
	default:
		return "", nil, fmt.Errorf("unexpected trustline asset type %d", a.Type)
	}
}

// trustlineKey is the identity of a trustline row: account and asset.
func trustlineKey(k xdr.LedgerKeyTrustLine) ([]any, error) {
	id, err := k.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}
	asset, _, err := trustlineAsset(k.Asset)
	if err != nil {
		return nil, err
	}
	return []any{id, asset}, nil
}

func trustlineValues(e xdr.LedgerEntry) ([]any, error) {
	tl := e.Data.MustTrustLine()
	id, err := tl.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}
	asset, pool, err := trustlineAsset(tl.Asset)
	if err != nil {
		return nil, err
	}
	sponsor, err := sponsorOf(e)
	if err != nil {
		return nil, err
	}
	liab := tl.Liabilities()
	var poolID any
	if pool != nil {
		poolID = pool
	}
	return []any{
		id, asset, poolID, int64(tl.Balance),
		int64(tl.Limit), int32(tl.Flags), int64(liab.Buying),
		int64(liab.Selling), sponsor, int64(e.LastModifiedLedgerSeq),
	}, nil
}
