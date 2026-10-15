package clickhouse

import (
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode"

	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
)

// table is one history table: its ledger column (for the delete above the
// cursor) and, unless a materialized view fills it, its insert columns,
// built column by column from a batch.
type table struct {
	name   string
	seqCol string
	cols   []string
	build  func(batch []*explorer.Prepared) ([]any, int, error) // columns, row count
}

// tables in insert order; transfers_by_account is filled by its view, and
// daily_stats by its refreshable view (daily_stats.sql), from these.
var tables = []table{
	{name: "ledgers", seqCol: "sequence", cols: []string{"sequence", "hash", "prev_hash", "closed_at", "protocol_version",
		"tx_count", "failed_tx_count", "op_count", "successful_op_count", "base_fee", "base_reserve", "max_tx_set_size",
		"total_coins", "fee_pool", "soroban_state_size", "source", "core_version"}, build: ledgerColumns},
	{name: "transactions", seqCol: "ledger_sequence", cols: []string{"ledger_sequence", "closed_at", "tx_index", "id", "hash", "inner_hash",
		"source_account", "source_account_muxed", "account_sequence", "fee_account", "fee_account_muxed", "max_fee", "fee_charged",
		"successful", "result_code", "op_count", "memo_type", "memo", "soroban"}, build: transactionColumns},
	{name: "operations", seqCol: "ledger_sequence", cols: []string{"ledger_sequence", "closed_at", "tx_index", "op_index", "id", "type",
		"type_name", "source_account", "source_account_muxed", "details", "successful", "result_code", "return_value"}, build: operationColumns},
	{name: "events", seqCol: "ledger_sequence", cols: []string{"ledger_sequence", "closed_at", "tx_index", "op_index", "event_index",
		"stage", "type", "contract_id", "topics", "data"}, build: eventColumns},
	{name: "token_transfers", seqCol: "ledger_sequence", cols: []string{"ledger_sequence", "closed_at", "tx_index", "op_index", "position",
		"kind", "asset", "contract_id", "from_address", "to_address", "amount", "to_muxed_info"}, build: transferColumns},
	{name: "transfers_by_account", seqCol: "ledger_sequence"},
	{name: "tx_by_hash", seqCol: "ledger_sequence", cols: []string{"prefix", "ledger_sequence", "tx_index"}, build: txByHashColumns},
	{name: "account_activity", seqCol: "ledger_sequence", cols: []string{"account", "ledger_sequence", "closed_at", "tx_index", "roles"},
		build: activityColumns},
	{name: "contract_calls", seqCol: "ledger_sequence", cols: []string{"contract_id", "ledger_sequence", "closed_at", "tx_index", "op_index",
		"function", "caller", "successful", "return_value"}, build: callColumns},
	{name: "contract_creations", seqCol: "ledger_sequence", cols: []string{"contract_id", "ledger_sequence", "closed_at", "tx_index", "op_index",
		"creator", "via_contract", "executable", "wasm_hash", "asset"}, build: creationColumns},
}

func (t table) insert() string {
	return "INSERT INTO " + t.name + " (" + strings.Join(t.cols, ", ") + ")"
}

func ledgerColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := len(batch)
	var (
		seq, proto, txs, failed, ops, okOps, baseFee, reserve, maxSet = mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n), mk[uint32](n)
		hash, prev                                                    = mk[string](n), mk[string](n)
		closed                                                        = mk[time.Time](n)
		coins, pool                                                   = mk[int64](n), mk[int64](n)
		soroban                                                       = mk[uint64](n)
		source, core                                                  = mk[string](n), mk[string](n)
	)
	for _, p := range batch {
		l := p.Ledger
		seq, hash, prev, closed = append(seq, l.Sequence), append(hash, string(l.Hash[:])), append(prev, string(l.PrevHash[:])), append(closed, l.ClosedAt)
		proto, txs, failed, ops, okOps = append(proto, l.ProtocolVersion), append(txs, l.TxCount), append(failed, l.FailedTxCount), append(ops, l.OpCount), append(okOps, l.SuccessfulOpCount)
		baseFee, reserve, maxSet = append(baseFee, l.BaseFee), append(reserve, l.BaseReserve), append(maxSet, l.MaxTxSetSize)
		coins, pool = append(coins, l.TotalCoins), append(pool, l.FeePool)
		var s uint64
		if l.SorobanStateSize != nil {
			s = *l.SorobanStateSize
		}
		soroban, source, core = append(soroban, s), append(source, l.Source), append(core, l.CoreVersion)
	}
	return []any{seq, hash, prev, closed, proto, txs, failed, ops, okOps, baseFee, reserve, maxSet, coins, pool, soroban, source, core}, n, nil
}

func transactionColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Transactions)
	}
	var (
		seq, idx, opCount                     = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                                = mk[time.Time](n)
		id, accSeq, maxFee, feeCharged        = mk[int64](n), mk[int64](n), mk[int64](n), mk[int64](n)
		hash, inner                           = mk[string](n), mk[string](n)
		src, srcMuxed, feeAcc, feeMuxed, memo = mk[string](n), mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		memoType                              = mk[string](n)
		ok, soroban                           = mk[bool](n), mk[bool](n)
		result                                = mk[int32](n)
	)
	for _, p := range batch {
		for _, t := range p.Transactions {
			var in xdr.Hash
			if t.InnerHash != nil {
				in = *t.InnerHash
			}
			var m string
			if t.Memo != nil {
				m = *t.Memo
			}
			seq, closed, idx, id = append(seq, t.LedgerSequence), append(closed, p.Ledger.ClosedAt), append(idx, t.TxIndex), append(id, t.ID)
			hash, inner = append(hash, string(t.Hash[:])), append(inner, string(in[:]))
			src, srcMuxed, accSeq = append(src, t.SourceAccount), append(srcMuxed, t.SourceAccountMuxed), append(accSeq, t.AccountSequence)
			feeAcc, feeMuxed, maxFee, feeCharged = append(feeAcc, t.FeeAccount), append(feeMuxed, t.FeeAccountMuxed), append(maxFee, t.MaxFee), append(feeCharged, t.FeeCharged)
			ok, result, opCount = append(ok, t.Successful), append(result, t.ResultCode), append(opCount, t.OpCount)
			memoType, memo, soroban = append(memoType, t.MemoType), append(memo, m), append(soroban, t.Soroban)
		}
	}
	return []any{seq, closed, idx, id, hash, inner, src, srcMuxed, accSeq, feeAcc, feeMuxed, maxFee, feeCharged, ok, result, opCount, memoType, memo, soroban}, n, nil
}

func operationColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Operations)
	}
	var (
		seq, txIdx, opIdx            = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                       = mk[time.Time](n)
		id                           = mk[int64](n)
		typ                          = mk[uint8](n)
		name, src, srcMuxed, details = mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		ok                           = mk[bool](n)
		result, ret                  = mk[string](n), mk[string](n)
	)
	for _, p := range batch {
		for _, o := range p.Operations {
			if o.Type < 0 || o.Type > 255 {
				return nil, 0, fmt.Errorf("ledger %d tx %d op %d: operation type %d does not fit UInt8", o.LedgerSequence, o.TxIndex, o.OpIndex, o.Type)
			}
			seq, closed, txIdx, opIdx, id = append(seq, o.LedgerSequence), append(closed, p.Ledger.ClosedAt), append(txIdx, o.TxIndex), append(opIdx, o.OpIndex), append(id, o.ID)
			typ, name = append(typ, uint8(o.Type)), append(name, OperationTypeName(o.Type))
			src, srcMuxed, details, ok = append(src, o.SourceAccount), append(srcMuxed, o.SourceAccountMuxed), append(details, o.Details), append(ok, o.Successful)
			result, ret = append(result, o.ResultCode), append(ret, o.ReturnValue)
		}
	}
	return []any{seq, closed, txIdx, opIdx, id, typ, name, src, srcMuxed, details, ok, result, ret}, n, nil
}

func eventColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Events)
	}
	var (
		seq, txIdx, evIdx          = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                     = mk[time.Time](n)
		opIdx                      = mk[int32](n)
		stage, typ, contract, data = mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		topics                     = mk[[]string](n)
	)
	for _, p := range batch {
		for _, e := range p.Events {
			seq, closed, txIdx, opIdx, evIdx = append(seq, e.LedgerSequence), append(closed, p.Ledger.ClosedAt), append(txIdx, e.TxIndex), append(opIdx, e.OpIndex), append(evIdx, e.EventIndex)
			stage, typ, contract = append(stage, e.Stage), append(typ, e.Type), append(contract, e.ContractID)
			topics, data = append(topics, append([]string{}, e.Topics...)), append(data, e.Data)
		}
	}
	return []any{seq, closed, txIdx, opIdx, evIdx, stage, typ, contract, topics, data}, n, nil
}

func transferColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Transfers)
	}
	var (
		seq, txIdx, pos                        = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                                 = mk[time.Time](n)
		opIdx                                  = mk[int32](n)
		kind, asset, contract, from, to, muxed = mk[string](n), mk[string](n), mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		amount                                 = mk[*big.Int](n)
	)
	for _, p := range batch {
		for _, t := range p.Transfers {
			a, ok := new(big.Int).SetString(t.Amount, 10)
			if !ok || a.BitLen() > 127 {
				return nil, 0, fmt.Errorf("ledger %d tx %d op %d: transfer amount %q is not an Int128", t.LedgerSequence, t.TxIndex, t.OpIndex, t.Amount)
			}
			seq, closed, txIdx, opIdx, pos = append(seq, t.LedgerSequence), append(closed, p.Ledger.ClosedAt), append(txIdx, t.TxIndex), append(opIdx, t.OpIndex), append(pos, t.Position)
			kind, asset, contract = append(kind, t.Kind), append(asset, t.Asset), append(contract, t.ContractID)
			from, to, amount, muxed = append(from, t.From), append(to, t.To), append(amount, a), append(muxed, t.ToMuxedInfo)
		}
	}
	return []any{seq, closed, txIdx, opIdx, pos, kind, asset, contract, from, to, amount, muxed}, n, nil
}

// txByHashColumns: every transaction by the first 8 bytes of its hash, and a
// fee bump's also by its inner transaction's.
func txByHashColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Transactions)
	}
	prefix, seq, idx := mk[string](n), mk[uint32](n), mk[uint32](n)
	for _, p := range batch {
		for _, t := range p.Transactions {
			prefix, seq, idx = append(prefix, string(t.Hash[:8])), append(seq, t.LedgerSequence), append(idx, t.TxIndex)
			if t.InnerHash != nil {
				prefix, seq, idx = append(prefix, string(t.InnerHash[:8])), append(seq, t.LedgerSequence), append(idx, t.TxIndex)
			}
		}
	}
	return []any{prefix, seq, idx}, len(prefix), nil
}

func activityColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Activity)
	}
	account, seq, closed, idx, roles := mk[string](n), mk[uint32](n), mk[time.Time](n), mk[uint32](n), mk[uint8](n)
	for _, p := range batch {
		for _, a := range p.Activity {
			account, seq, closed = append(account, a.Account), append(seq, p.Ledger.Sequence), append(closed, p.Ledger.ClosedAt)
			idx, roles = append(idx, a.TxIndex), append(roles, a.Roles)
		}
	}
	return []any{account, seq, closed, idx, roles}, n, nil
}

func callColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Calls)
	}
	var (
		contract, function, caller, ret = mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		seq, txIdx, opIdx               = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                          = mk[time.Time](n)
		ok                              = mk[bool](n)
	)
	for _, p := range batch {
		for _, c := range p.Calls {
			contract, seq, closed, txIdx, opIdx = append(contract, c.ContractID), append(seq, p.Ledger.Sequence), append(closed, p.Ledger.ClosedAt), append(txIdx, c.TxIndex), append(opIdx, c.OpIndex)
			function, caller, ok, ret = append(function, c.Function), append(caller, c.Caller), append(ok, c.Successful), append(ret, c.ReturnValue)
		}
	}
	return []any{contract, seq, closed, txIdx, opIdx, function, caller, ok, ret}, n, nil
}

func creationColumns(batch []*explorer.Prepared) ([]any, int, error) {
	n := 0
	for _, p := range batch {
		n += len(p.Creations)
	}
	var (
		contract, creator, via, executable, wasm, asset = mk[string](n), mk[string](n), mk[string](n), mk[string](n), mk[string](n), mk[string](n)
		seq, txIdx, opIdx                               = mk[uint32](n), mk[uint32](n), mk[uint32](n)
		closed                                          = mk[time.Time](n)
	)
	for _, p := range batch {
		for _, c := range p.Creations {
			var h [32]byte
			copy(h[:], c.WasmHash)
			contract, seq, closed, txIdx, opIdx = append(contract, c.ContractID), append(seq, p.Ledger.Sequence), append(closed, p.Ledger.ClosedAt), append(txIdx, c.TxIndex), append(opIdx, c.OpIndex)
			creator, via, executable, wasm, asset = append(creator, c.Creator), append(via, c.ViaContract), append(executable, c.Executable), append(wasm, string(h[:])), append(asset, c.Asset)
		}
	}
	return []any{contract, seq, closed, txIdx, opIdx, creator, via, executable, wasm, asset}, n, nil
}

func mk[T any](n int) []T { return make([]T, 0, n) }

// OperationTypeName is a type's name in snake case:
// OperationTypePathPaymentStrictReceive → path_payment_strict_receive.
func OperationTypeName(t int32) string {
	s := strings.TrimPrefix(xdr.OperationType(t).String(), "OperationType")
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
