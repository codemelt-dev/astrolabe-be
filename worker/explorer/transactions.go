package explorer

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TransactionRow is one transaction, as the clickhouse pipeline stores it
// (clickhouse/transactions.sql).
type TransactionRow struct {
	LedgerSequence     uint32
	TxIndex            uint32 // 1-based (= LedgerTransaction.Index)
	ID                 int64  // toid(ledger, tx_index, 0)
	Hash               xdr.Hash
	InnerHash          *xdr.Hash
	SourceAccount      string
	SourceAccountMuxed string
	AccountSequence    int64
	FeeAccount         string
	FeeAccountMuxed    string
	MaxFee             int64
	FeeCharged         int64
	Successful         bool
	ResultCode         int32
	OpCount            uint32
	MemoType           string
	Memo               *string
	Soroban            bool
}

func (r *TransactionRow) size() int64 {
	n := int64(250)
	if r.Memo != nil {
		n += int64(len(*r.Memo))
	}
	return n
}

// accountStrings returns the G… address of a (possibly muxed) account and,
// when it is muxed, its M… address.
func accountStrings(m xdr.MuxedAccount) (base, muxed string, err error) {
	aid := m.ToAccountId()
	if base, err = aid.GetAddress(); err != nil {
		return "", "", err
	}
	if m.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		if muxed, err = m.GetAddress(); err != nil {
			return "", "", err
		}
	}
	return base, muxed, nil
}

func transactionRow(tx ingest.LedgerTransaction, seq uint32) (TransactionRow, error) {
	r := TransactionRow{
		LedgerSequence:  seq,
		TxIndex:         tx.Index,
		ID:              tx.ID(),
		Hash:            tx.Hash,
		AccountSequence: tx.Envelope.SeqNum(),
		Successful:      tx.Successful(),
		ResultCode:      int32(tx.Result.Result.Result.Code),
		OpCount:         tx.OperationCount(),
		Soroban:         tx.IsSorobanTx(),
	}
	var err error
	if r.SourceAccount, r.SourceAccountMuxed, err = accountStrings(tx.Envelope.SourceAccount()); err != nil {
		return r, fmt.Errorf("source account: %w", err)
	}
	if r.FeeAccount, r.FeeAccountMuxed, err = accountStrings(tx.Envelope.FeeAccount()); err != nil {
		return r, fmt.Errorf("fee account: %w", err)
	}
	if tx.Envelope.IsFeeBump() {
		inner := tx.Result.InnerHash()
		r.InnerHash = &inner
		r.MaxFee = tx.Envelope.FeeBumpFee()
	} else {
		r.MaxFee = int64(tx.Envelope.Fee())
	}
	r.FeeCharged, _ = tx.FeeCharged()
	r.MemoType, r.Memo, err = memoOf(tx.Envelope.Memo())
	if err != nil {
		return r, err
	}
	return r, nil
}

// memoOf returns the memo type and value: text (made Postgres-safe), id
// (decimal), hash and return (hex).
func memoOf(m xdr.Memo) (string, *string, error) {
	var v string
	switch m.Type {
	case xdr.MemoTypeMemoNone:
		return "none", nil, nil
	case xdr.MemoTypeMemoText:
		v = cleanText(m.MustText())
		return "text", &v, nil
	case xdr.MemoTypeMemoId:
		v = strconv.FormatUint(uint64(m.MustId()), 10)
		return "id", &v, nil
	case xdr.MemoTypeMemoHash:
		h := m.MustHash()
		v = hex.EncodeToString(h[:])
		return "hash", &v, nil
	case xdr.MemoTypeMemoReturn:
		h := m.MustRetHash()
		v = hex.EncodeToString(h[:])
		return "return", &v, nil
	default:
		return "", nil, fmt.Errorf("unknown memo type %d", m.Type)
	}
}
