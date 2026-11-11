package explorer

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TradeRow is one fill of the DEX, as the clickhouse pipeline stores it
// (clickhouse/trades.sql): an offer an operation matched in the order
// book, or a liquidity pool it swapped with. It comes from the operation's
// result (ClaimAtom), the original transaction result, as in Horizon's
// trades. Before protocol 18 the result names the seller by its key
// (ClaimAtom V0); it is the same order-book fill.
type TradeRow struct {
	TxIndex      uint32 // 1-based
	OpIndex      uint32 // 0-based
	Position     uint32 // the fill's index in the operation's result
	Venue        string // VenueOrderBook or VenuePool
	OfferID      int64  // 0 for a pool
	Seller       string // the offer's owner (G…), or the pool (L…)
	Buyer        string // the operation's source
	SoldAsset    string // what the seller gave: native or CODE:ISSUER
	SoldAmount   int64
	BoughtAsset  string // what the seller received
	BoughtAmount int64
}

// Trade venues (trades.venue).
const (
	VenueOrderBook = "orderbook"
	VenuePool      = "pool"
)

func (r *TradeRow) size() int64 {
	return 100 + int64(len(r.Seller)+len(r.Buyer)+len(r.SoldAsset)+len(r.BoughtAsset))
}

// tradeRowsOf returns a successful transaction's fills, operation by
// operation. A fill that moved nothing (both amounts 0) is left out.
func tradeRowsOf(tx ingest.LedgerTransaction) ([]TradeRow, error) {
	if !tx.Successful() {
		return nil, nil
	}
	results, ok := tx.Result.OperationResults()
	if !ok {
		return nil, nil
	}
	ops := tx.Envelope.Operations()
	if len(results) != len(ops) {
		return nil, fmt.Errorf("%d operation results for %d operations", len(results), len(ops))
	}
	var rows []TradeRow
	for i, res := range results {
		atoms := claimAtoms(res)
		if len(atoms) == 0 {
			continue
		}
		src := tx.Envelope.SourceAccount()
		if ops[i].SourceAccount != nil {
			src = *ops[i].SourceAccount
		}
		buyer, _, err := accountStrings(src)
		if err != nil {
			return nil, fmt.Errorf("operation %d source: %w", i, err)
		}
		for j, a := range atoms {
			if a.AmountSold() == 0 && a.AmountBought() == 0 {
				continue
			}
			r := TradeRow{TxIndex: tx.Index, OpIndex: uint32(i), Position: uint32(j), Buyer: buyer,
				SoldAmount: int64(a.AmountSold()), BoughtAmount: int64(a.AmountBought())}
			if r.SoldAsset, err = assetName(a.AssetSold()); err != nil {
				return nil, fmt.Errorf("operation %d fill %d: sold asset: %w", i, j, err)
			}
			if r.BoughtAsset, err = assetName(a.AssetBought()); err != nil {
				return nil, fmt.Errorf("operation %d fill %d: bought asset: %w", i, j, err)
			}
			switch a.Type {
			case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
				id := a.MustLiquidityPool().LiquidityPoolId
				r.Venue = VenuePool
				if r.Seller, err = strkey.Encode(strkey.VersionByteLiquidityPool, id[:]); err != nil {
					return nil, fmt.Errorf("operation %d fill %d: pool: %w", i, j, err)
				}
			case xdr.ClaimAtomTypeClaimAtomTypeV0, xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
				r.Venue, r.OfferID = VenueOrderBook, int64(a.OfferId())
				seller := a.SellerId()
				if r.Seller, err = seller.GetAddress(); err != nil {
					return nil, fmt.Errorf("operation %d fill %d: seller: %w", i, j, err)
				}
			default:
				return nil, fmt.Errorf("operation %d fill %d: unknown ClaimAtom type %d", i, j, a.Type)
			}
			rows = append(rows, r)
		}
	}
	return rows, nil
}

// claimAtoms are the fills in an operation's result: those of the offer
// operations and of path payments; none for any other operation.
func claimAtoms(res xdr.OperationResult) []xdr.ClaimAtom {
	tr, ok := res.GetTr()
	if !ok {
		return nil
	}
	switch tr.Type {
	case xdr.OperationTypeManageSellOffer:
		if s, ok := tr.MustManageSellOfferResult().GetSuccess(); ok {
			return s.OffersClaimed
		}
	case xdr.OperationTypeCreatePassiveSellOffer:
		if s, ok := tr.MustCreatePassiveSellOfferResult().GetSuccess(); ok {
			return s.OffersClaimed
		}
	case xdr.OperationTypeManageBuyOffer:
		if s, ok := tr.MustManageBuyOfferResult().GetSuccess(); ok {
			return s.OffersClaimed
		}
	case xdr.OperationTypePathPaymentStrictReceive:
		if s, ok := tr.MustPathPaymentStrictReceiveResult().GetSuccess(); ok {
			return s.Offers
		}
	case xdr.OperationTypePathPaymentStrictSend:
		if s, ok := tr.MustPathPaymentStrictSendResult().GetSuccess(); ok {
			return s.Offers
		}
	}
	return nil
}

// CheckTrades checks a processed ledger's trades against its token
// movements: each fill's two amounts must appear among its operation's
// transfers, with the same assets. A movement is a transfer, or a mint or
// a burn when a side of the fill is the asset's issuer. Both come from the
// same ledger by different routes: the trades from the operations' results,
// the movements from the events in the transaction meta.
func CheckTrades(p *Prepared) error {
	type leg struct {
		tx     uint32
		op     int32
		asset  string
		amount string
	}
	moved := map[leg]int{}
	for _, t := range p.Transfers {
		switch t.Kind {
		case "transfer", "mint", "burn":
			moved[leg{t.TxIndex, t.OpIndex, t.Asset, t.Amount}]++
		}
	}
	for _, t := range p.Trades {
		for _, l := range []leg{
			{t.TxIndex, int32(t.OpIndex), t.SoldAsset, fmt.Sprint(t.SoldAmount)},
			{t.TxIndex, int32(t.OpIndex), t.BoughtAsset, fmt.Sprint(t.BoughtAmount)},
		} {
			if l.amount == "0" {
				continue
			}
			if moved[l] == 0 {
				return fmt.Errorf("ledger %d tx %d operation %d fill %d: no token movement of %s %s",
					p.Ledger.Sequence, t.TxIndex, t.OpIndex, t.Position, l.amount, l.asset)
			}
			moved[l]--
		}
	}
	return nil
}
