package explorer

import (
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledger"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
)

// LedgerRow is a row of explorer.ledgers (ledgers.sql).
type LedgerRow struct {
	Sequence          uint32
	Hash              xdr.Hash
	PrevHash          xdr.Hash
	ClosedAt          time.Time
	ProtocolVersion   uint32
	TxCount           uint32
	FailedTxCount     uint32
	OpCount           uint32
	SuccessfulOpCount uint32
	BaseFee           uint32
	BaseReserve       uint32
	MaxTxSetSize      uint32
	TotalCoins        int64
	FeePool           int64
	SorobanStateSize  *uint64
	// Source is the producer of the bytes: "galexie" (our lake or archive)
	// or "aws_lake".
	Source string
	// CoreVersion is the object's x-amz-meta-core-version, when known.
	CoreVersion string
}

// Ledger sources, as stored in explorer.ledgers.source.
const (
	SourceGalexie = "galexie"
	SourceAWSLake = "aws_lake"
)

var ledgersTable = table{
	name:   "ledgers",
	seqCol: "sequence",
	key:    []string{"sequence"},
	cols: []column{
		{"sequence", "bigint"}, {"hash", "bytea"}, {"prev_hash", "bytea"}, {"closed_at", "timestamp with time zone"},
		{"protocol_version", "integer"}, {"tx_count", "integer"}, {"failed_tx_count", "integer"},
		{"op_count", "integer"}, {"successful_op_count", "integer"}, {"base_fee", "integer"},
		{"base_reserve", "integer"}, {"max_tx_set_size", "integer"}, {"total_coins", "bigint"},
		{"fee_pool", "bigint"}, {"soroban_state_size", "bigint"}, {"source", "text"}, {"core_version", "text"},
	},
}

func (r *LedgerRow) values() []any {
	var stateSize any
	if r.SorobanStateSize != nil {
		stateSize = int64(*r.SorobanStateSize)
	}
	return []any{
		int64(r.Sequence), r.Hash[:], r.PrevHash[:], r.ClosedAt,
		int32(r.ProtocolVersion), int32(r.TxCount), int32(r.FailedTxCount),
		int32(r.OpCount), int32(r.SuccessfulOpCount), int32(r.BaseFee),
		int32(r.BaseReserve), int32(r.MaxTxSetSize), r.TotalCoins,
		r.FeePool, stateSize, r.Source, nullString(r.CoreVersion),
	}
}

type ledgerRows []LedgerRow

func (r ledgerRows) Len() int           { return len(r) }
func (r ledgerRows) Values(i int) []any { return r[i].values() }

// ledgerSource maps a read source to the producer recorded in the row: the
// lake and the archive hold our Galexie's exports (1:1), the AWS lake is
// another producer's.
func ledgerSource(readSource string) string {
	if readSource == sep54.SourceAWS {
		return SourceAWSLake
	}
	return SourceGalexie
}

// ledgerRow extracts the ledger row (via the SDK's ingest/ledger helpers).
func ledgerRow(lcm xdr.LedgerCloseMeta, h sep54.Header, readSource string, metadata map[string]string) LedgerRow {
	successTx, totalTx := ledger.TransactionCounts(lcm)
	successOps, totalOps := ledger.OperationCounts(lcm)
	r := LedgerRow{
		Sequence:          h.Sequence,
		Hash:              h.Hash,
		PrevHash:          h.PreviousHash,
		ClosedAt:          ledger.ClosedAt(lcm).UTC(),
		ProtocolVersion:   ledger.LedgerVersion(lcm),
		TxCount:           totalTx,
		FailedTxCount:     totalTx - successTx,
		OpCount:           totalOps,
		SuccessfulOpCount: successOps,
		BaseFee:           ledger.BaseFee(lcm),
		BaseReserve:       ledger.BaseReserve(lcm),
		MaxTxSetSize:      ledger.MaxTxSetSize(lcm),
		TotalCoins:        ledger.TotalCoins(lcm),
		FeePool:           ledger.FeePool(lcm),
		Source:            ledgerSource(readSource),
		CoreVersion:       cleanText(metadata["core-version"]),
	}
	if size, ok := ledger.TotalByteSizeOfLiveSorobanState(lcm); ok {
		r.SorobanStateSize = &size
	}
	return r
}
