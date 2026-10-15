package explorer

import (
	"encoding/hex"
	"fmt"
	"strconv"

	assetproto "github.com/stellar/go-stellar-sdk/asset"
	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TransferRow is one token movement from the SDK's token-transfer
// processor, as the clickhouse pipeline stores it
// (clickhouse/transfers.sql).
type TransferRow struct {
	LedgerSequence uint32
	TxIndex        uint32 // 1-based (= the processor's TransactionIndex)
	OpIndex        int32  // 0-based (= OperationIndex - 1); -1 for fee events
	Position       uint32 // 0-based within (tx_index, op_index), in slice order
	Kind           string // transfer, mint, burn, clawback or fee
	Asset          string // "native", "CODE:ISSUER", or "" for non-SAC tokens
	ContractID     string
	From, To       string
	Amount         string // decimal, base units
	ToMuxedInfo    string // JSON, or ""
}

func (r *TransferRow) size() int64 {
	return 120 + int64(len(r.Asset)+len(r.ContractID)+len(r.From)+len(r.To)+len(r.Amount)+len(r.ToMuxedInfo))
}

// transferRowsOf runs the SDK's token-transfer processor over the ledger
// (unified CAP-67 events) and numbers each event within its
// (tx_index, op_index) group.
func transferRowsOf(passphrase string, lcm xdr.LedgerCloseMeta) ([]TransferRow, error) {
	events, err := token_transfer.NewEventsProcessorForUnifiedEvents(passphrase).EventsFromLedger(lcm)
	if err != nil {
		return nil, fmt.Errorf("token transfers: %w", err)
	}
	type group struct {
		tx uint32
		op int32
	}
	positions := map[group]uint32{}
	rows := make([]TransferRow, 0, len(events))
	for i, ev := range events {
		meta := ev.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("token transfer %d has no meta", i)
		}
		r := TransferRow{
			LedgerSequence: meta.GetLedgerSequence(),
			TxIndex:        meta.GetTransactionIndex(),
			OpIndex:        -1,
			ContractID:     meta.GetContractAddress(),
		}
		if meta.OperationIndex != nil {
			if *meta.OperationIndex == 0 {
				return nil, fmt.Errorf("token transfer %d: operation index 0 (want 1-based)", i)
			}
			r.OpIndex = int32(*meta.OperationIndex) - 1
		}
		g := group{r.TxIndex, r.OpIndex}
		r.Position = positions[g]
		positions[g]++

		var a *assetproto.Asset
		switch e := ev.GetEvent().(type) {
		case *token_transfer.TokenTransferEvent_Transfer:
			r.Kind, r.From, r.To, r.Amount, a = "transfer", e.Transfer.GetFrom(), e.Transfer.GetTo(), e.Transfer.GetAmount(), e.Transfer.GetAsset()
		case *token_transfer.TokenTransferEvent_Mint:
			r.Kind, r.To, r.Amount, a = "mint", e.Mint.GetTo(), e.Mint.GetAmount(), e.Mint.GetAsset()
		case *token_transfer.TokenTransferEvent_Burn:
			r.Kind, r.From, r.Amount, a = "burn", e.Burn.GetFrom(), e.Burn.GetAmount(), e.Burn.GetAsset()
		case *token_transfer.TokenTransferEvent_Clawback:
			r.Kind, r.From, r.Amount, a = "clawback", e.Clawback.GetFrom(), e.Clawback.GetAmount(), e.Clawback.GetAsset()
		case *token_transfer.TokenTransferEvent_Fee:
			r.Kind, r.From, r.Amount, a = "fee", e.Fee.GetFrom(), e.Fee.GetAmount(), e.Fee.GetAsset()
		default:
			return nil, fmt.Errorf("token transfer %d: unknown event %T", i, e)
		}
		if _, err := strconv.ParseFloat(r.Amount, 64); err != nil || r.Amount == "" {
			return nil, fmt.Errorf("token transfer %d: bad amount %q", i, r.Amount)
		}
		r.Asset = assetString(a)
		r.ToMuxedInfo = muxedInfoJSON(meta.GetToMuxedInfo())
		rows = append(rows, r)
	}
	return rows, nil
}

func assetString(a *assetproto.Asset) string {
	switch {
	case a == nil:
		return ""
	case a.GetNative():
		return "native"
	case a.GetIssuedAsset() != nil:
		return cleanText(a.GetIssuedAsset().GetAssetCode() + ":" + a.GetIssuedAsset().GetIssuer())
	}
	return ""
}

// muxedInfoJSON encodes CAP-67 destination muxing info: {"id":"…"},
// {"text":…} or {"hash":"hex"}.
func muxedInfoJSON(m *token_transfer.MuxedInfo) string {
	if m == nil {
		return ""
	}
	var b []byte
	switch c := m.GetContent().(type) {
	case *token_transfer.MuxedInfo_Id:
		b = append(b, `{"id":`...)
		b = appendQuotedUint(b, c.Id)
	case *token_transfer.MuxedInfo_Text:
		b = append(b, `{"text":`...)
		b = appendXDRString(b, c.Text)
	case *token_transfer.MuxedInfo_Hash:
		b = append(b, `{"hash":"`...)
		b = hex.AppendEncode(b, c.Hash)
		b = append(b, '"')
	default:
		return ""
	}
	return string(append(b, '}'))
}
