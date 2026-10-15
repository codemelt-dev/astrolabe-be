package explorer

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// liquidity_pools: the AMM pools (state_liquidity_pools.sql).

var liquidityPoolsTable = stateTable{table: table{
	name: "liquidity_pools",
	key:  []string{"pool_id"},
	cols: []column{
		{"pool_id", "bytea"}, {"asset_a", "text"}, {"asset_b", "text"}, {"fee_bp", "integer"},
		{"reserve_a", "bigint"}, {"reserve_b", "bigint"}, {"total_shares", "bigint"}, {"trustlines", "bigint"},
		{"last_modified_ledger", "bigint"},
	},
}}

func liquidityPoolValues(e xdr.LedgerEntry) ([]any, error) {
	lp := e.Data.MustLiquidityPool()
	cp, ok := lp.Body.GetConstantProduct()
	if !ok {
		return nil, fmt.Errorf("liquidity pool %x: unknown type %d", lp.LiquidityPoolId[:], lp.Body.Type)
	}
	a, err := assetName(cp.Params.AssetA)
	if err != nil {
		return nil, err
	}
	b, err := assetName(cp.Params.AssetB)
	if err != nil {
		return nil, err
	}
	return []any{
		lp.LiquidityPoolId[:], a, b, int32(cp.Params.Fee),
		int64(cp.ReserveA), int64(cp.ReserveB), int64(cp.TotalPoolShares), int64(cp.PoolSharesTrustLineCount),
		int64(e.LastModifiedLedgerSeq),
	}, nil
}
