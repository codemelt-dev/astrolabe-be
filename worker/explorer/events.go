package explorer

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// EventRow is one event, as the clickhouse pipeline stores it
// (clickhouse/events.sql).
type EventRow struct {
	LedgerSequence uint32
	TxIndex        uint32 // 1-based
	OpIndex        int32  // 0-based; -1 for transaction-level (fee) events
	EventIndex     uint32
	Stage          string // fee, refund or operation
	Type           string // contract or system
	ContractID     string // C…, or "" when absent
	// Topics are every topic, as JSON (ScValJSON); pubnet has events with
	// up to 6.
	Topics []string
	Data   string // JSON (ScValJSON)
}

// Event stages (events.stage).
const (
	StageFee       = "fee"
	StageRefund    = "refund"
	StageOperation = "operation"
)

func (r *EventRow) size() int64 {
	n := 150 + len(r.Data)
	for _, t := range r.Topics {
		n += len(t)
	}
	return int64(n)
}

// eventRowsOf extracts the CAP-67 unified events of one transaction:
// transaction-level fee events (op_index -1) and each operation's
// contract events. Diagnostic events are not stored.
func eventRowsOf(tx ingest.LedgerTransaction, seq uint32) ([]EventRow, error) {
	evs, err := tx.GetTransactionEvents()
	if err != nil {
		return nil, err
	}
	var rows []EventRow
	for j, te := range evs.TransactionEvents {
		stage, err := transactionEventStage(te.Stage)
		if err != nil {
			return nil, err
		}
		r, err := eventRow(te.Event, seq, tx.Index, -1, uint32(j), stage)
		if err != nil {
			return nil, fmt.Errorf("transaction event %d: %w", j, err)
		}
		rows = append(rows, r)
	}
	for op, list := range evs.OperationEvents {
		for j, ev := range list {
			r, err := eventRow(ev, seq, tx.Index, int32(op), uint32(j), StageOperation)
			if err != nil {
				return nil, fmt.Errorf("operation %d event %d: %w", op, j, err)
			}
			rows = append(rows, r)
		}
	}
	return rows, nil
}

// transactionEventStage maps CAP-67 stages to events.stage: fees are
// charged before all transactions, refunds come after the transaction (or,
// from protocol 23, after all transactions).
func transactionEventStage(s xdr.TransactionEventStage) (string, error) {
	switch s {
	case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
		return StageFee, nil
	case xdr.TransactionEventStageTransactionEventStageAfterTx,
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
		return StageRefund, nil
	default:
		return "", fmt.Errorf("unknown transaction event stage %d", s)
	}
}

func eventRow(ev xdr.ContractEvent, seq, txIndex uint32, opIndex int32, index uint32, stage string) (EventRow, error) {
	r := EventRow{
		LedgerSequence: seq,
		TxIndex:        txIndex,
		OpIndex:        opIndex,
		EventIndex:     index,
		Stage:          stage,
	}
	switch ev.Type {
	case xdr.ContractEventTypeContract:
		r.Type = "contract"
	case xdr.ContractEventTypeSystem:
		r.Type = "system"
	default:
		return r, fmt.Errorf("unexpected contract event type %d", ev.Type)
	}
	if ev.ContractId != nil {
		id, err := contractStrkey(*ev.ContractId)
		if err != nil {
			return r, err
		}
		r.ContractID = id
	}
	body, ok := ev.Body.GetV0()
	if !ok {
		return r, fmt.Errorf("unknown contract event body version %d", ev.Body.V)
	}
	var err error
	r.Topics = make([]string, len(body.Topics))
	for i, t := range body.Topics {
		if r.Topics[i], err = ScValJSON(t); err != nil {
			return r, fmt.Errorf("topic %d: %w", i, err)
		}
	}
	if r.Data, err = ScValJSON(body.Data); err != nil {
		return r, fmt.Errorf("data: %w", err)
	}
	return r, nil
}
