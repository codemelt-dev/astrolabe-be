// Package explorer is processLedger, shared by both pipelines, and the
// explorer pipeline's PostgreSQL side: the schema with its migrations (one
// .sql file per entity, next to the Go that writes it) and the Sink. The
// explorer pipeline keeps current state and one row per ledger; the
// clickhouse pipeline (package clickhouse) stores the history rows.
//
// Index conventions (everywhere):
//   - tx_index is 1-based (= LedgerTransaction.Index = the token-transfer
//     processor's TransactionIndex);
//   - op_index is 0-based, and -1 for transaction-level rows (fee and refund
//     events, token-transfer events without an operation);
//   - transaction id = toid(seq, tx_index, 0); operation id =
//     toid(seq, tx_index, op_index + 1), as in Horizon and stellar-etl.
package explorer

import (
	"errors"
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Prepared is one processed ledger: its row, and the history rows or the
// state changes the processor was asked for.
type Prepared struct {
	header sep54.Header
	Ledger LedgerRow
	// The history (HistoryProcessor only): what the clickhouse pipeline
	// stores.
	Transactions []TransactionRow
	Operations   []OperationRow
	Events       []EventRow
	Transfers    []TransferRow
	Activity     []ActivityRow
	Calls        []ContractCallRow
	Creations    []ContractCreationRow
	// State are the ledger's current-state changes (Processor only):
	// compacted changes, then evictions.
	State []StateChange
	size  int64
}

// Header implements sequencer.Prepared.
func (p *Prepared) Header() sep54.Header { return p.header }

// Size implements sequencer.Prepared: an estimate of the bytes the rows
// hold.
func (p *Prepared) Size() int64 { return p.size }

// rows returns the rows of historyTables[i].
func (p *Prepared) rows(i int) rowSet {
	switch historyTables[i] {
	case &ledgersTable:
		return ledgerRows{p.Ledger}
	}
	panic(fmt.Sprintf("no rows for table %d", i))
}

// RowCounts returns the number of rows per table name.
func (p *Prepared) RowCounts() map[string]int {
	out := make(map[string]int, len(historyTables))
	for i, t := range historyTables {
		out[t.name] = p.rows(i).Len()
	}
	return out
}

var _ sequencer.Prepared = (*Prepared)(nil)

// What a processor extracts besides the ledger row.
type mode uint8

const (
	withHistory mode = 1 << iota // transactions, operations, events, transfers and what derives from them
	withState                    // the current-state changes
)

// Processor returns the explorer pipeline's processLedger for the given
// network: a pure function from a decoded ledger to its row and its state
// changes. The passphrase is part of every transaction hash; with the wrong
// one the transaction reader fails.
func Processor(passphrase string) sequencer.Processor[*Prepared] {
	return func(l sequencer.Ledger) (*Prepared, error) {
		return process(passphrase, l, withState)
	}
}

// HistoryProcessor is the clickhouse pipeline's: the ledger row and every
// history row, no state.
func HistoryProcessor(passphrase string) sequencer.Processor[*Prepared] {
	return func(l sequencer.Ledger) (*Prepared, error) {
		return process(passphrase, l, withHistory)
	}
}

// LedgerProcessor is reindex's: the ledger row alone, the only history
// PostgreSQL keeps.
func LedgerProcessor(passphrase string) sequencer.Processor[*Prepared] {
	return func(l sequencer.Ledger) (*Prepared, error) {
		return process(passphrase, l, 0)
	}
}

// Process is the explorer pipeline's processLedger (Processor).
func Process(passphrase string, l sequencer.Ledger) (*Prepared, error) {
	return process(passphrase, l, withState)
}

// ProcessHistory is the clickhouse pipeline's (HistoryProcessor).
func ProcessHistory(passphrase string, l sequencer.Ledger) (*Prepared, error) {
	return process(passphrase, l, withHistory)
}

func process(passphrase string, l sequencer.Ledger, m mode) (*Prepared, error) {
	seq := l.Header.Sequence
	if l.LCM.LedgerSequence() != seq {
		return nil, fmt.Errorf("ledger %d: meta is for ledger %d", seq, l.LCM.LedgerSequence())
	}
	p := &Prepared{
		header: l.Header,
		Ledger: ledgerRow(l.LCM, l.Header, l.Source, l.Metadata),
		size:   512,
	}
	if m&withHistory != 0 {
		if err := p.history(passphrase, l.LCM); err != nil {
			return nil, err
		}
	}
	if m&withState != 0 {
		var err error
		if p.State, err = stateChangesOf(passphrase, l.LCM); err != nil {
			return nil, fmt.Errorf("ledger %d state: %w", seq, err)
		}
		for i := range p.State {
			p.size += p.State[i].size()
		}
	}
	return p, nil
}

// history extracts the ledger's history rows.
func (p *Prepared) history(passphrase string, lcm xdr.LedgerCloseMeta) error {
	seq := p.Ledger.Sequence
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return fmt.Errorf("ledger %d: %w", seq, err)
	}
	n := lcm.CountTransactions()
	p.Transactions = make([]TransactionRow, 0, n)
	p.Operations = make([]OperationRow, 0, 2*n)
	p.Events = make([]EventRow, 0, 2*n)
	activity := make([]activitySet, 0, n)
	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("ledger %d: read transaction: %w", seq, err)
		}
		row, err := transactionRow(tx, seq)
		if err != nil {
			return fmt.Errorf("ledger %d tx %d: %w", seq, tx.Index, err)
		}
		p.Transactions = append(p.Transactions, row)
		p.size += row.size()

		ops, err := operationRowsOf(tx, seq)
		if err != nil {
			return fmt.Errorf("ledger %d tx %d: %w", seq, tx.Index, err)
		}
		for i := range ops {
			p.size += ops[i].size()
		}
		p.Operations = append(p.Operations, ops...)

		evs, err := eventRowsOf(tx, seq)
		if err != nil {
			return fmt.Errorf("ledger %d tx %d events: %w", seq, tx.Index, err)
		}
		for i := range evs {
			p.size += evs[i].size()
		}
		p.Events = append(p.Events, evs...)

		calls, creations, err := contractRowsOf(tx, ops, passphrase)
		if err != nil {
			return fmt.Errorf("ledger %d tx %d contracts: %w", seq, tx.Index, err)
		}
		p.Calls = append(p.Calls, calls...)
		p.Creations = append(p.Creations, creations...)
		p.size += int64(len(calls)*200 + len(creations)*200)

		a, err := activityOf(tx, ops, passphrase)
		if err != nil {
			return fmt.Errorf("ledger %d tx %d activity: %w", seq, tx.Index, err)
		}
		activity = append(activity, a)
	}
	if uint32(len(p.Transactions)) != p.Ledger.TxCount {
		return fmt.Errorf("ledger %d: read %d transactions, the set has %d", seq, len(p.Transactions), p.Ledger.TxCount)
	}
	if p.Transfers, err = transferRowsOf(passphrase, lcm); err != nil {
		return fmt.Errorf("ledger %d: %w", seq, err)
	}
	for i := range p.Transfers {
		if p.Transfers[i].LedgerSequence != seq {
			return fmt.Errorf("ledger %d: token transfer for ledger %d", seq, p.Transfers[i].LedgerSequence)
		}
		p.size += p.Transfers[i].size()
	}
	p.Activity = activityRows(seq, activity, p.Transfers)
	p.size += int64(len(p.Activity) * 80)
	return nil
}
