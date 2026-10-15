package sequencer

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
)

// Ledger is one decoded and validated ledger: the input of a Processor.
// Its object passed the full decode: V2 only, sequence asserts
// and sha256(XDR(header)) == hash.
type Ledger struct {
	Header sep54.Header
	LCM    xdr.LedgerCloseMeta
	// Source is where the object was read from: sep54.SourceLake,
	// SourceArchive or SourceAWS.
	Source string
	// Metadata are the object's x-amz-meta-* values (unauthenticated,
	// informational only).
	Metadata map[string]string
	// Size is the compressed object size in bytes.
	Size int
}

// Prepared is a processed ledger, ready to be committed by a Sink.
type Prepared interface {
	// Header identifies the ledger and links it to its predecessor.
	Header() sep54.Header
	// Size approximates the memory the prepared rows hold; it bounds
	// batches (at most 512 MiB prepared per commit).
	Size() int64
}

// Sink is where a pipeline commits. Postgres is the v1 sink
// (explorer.Sink); a future second database is another Sink with its own
// cursor in that database.
type Sink[P Prepared] interface {
	// Cursor returns the last committed ledger and its hash.
	Cursor(ctx context.Context) (seq uint32, hash xdr.Hash, err error)
	// Commit atomically writes a contiguous batch and advances the cursor
	// to its last ledger, guarded by the cursor: the batch's
	// first ledger must follow the cursor and link to its hash. Errors
	// should be classified with Transient and Halt.
	Commit(ctx context.Context, batch []P) error
}

// Processor turns a ledger into prepared rows (processLedger).
// It must be pure: the same ledger always yields the same rows.
type Processor[P Prepared] func(Ledger) (P, error)

// HaltError stops a pipeline without advancing its cursor: every source
// failed validation for a ledger, the cursor's hash chain does not match,
// or the sink rejected data deterministically. Stellar has no reorgs, so a
// mismatch means corruption, the wrong network or mixed sources; an
// operator must act (galexie/README.md §7.4.3).
type HaltError struct {
	Seq uint32
	Err error
}

func (e *HaltError) Error() string {
	return fmt.Sprintf("pipeline halted at ledger %d: %v", e.Seq, e.Err)
}
func (e *HaltError) Unwrap() error { return e.Err }

// Halt marks err as halting the pipeline at ledger seq.
func Halt(seq uint32, err error) error { return &HaltError{Seq: seq, Err: err} }

// IsHalt reports whether err halts the pipeline.
func IsHalt(err error) bool {
	var he *HaltError
	return errors.As(err, &he)
}

// TransientError is a sink failure worth retrying as is: the database is
// unreachable, restarting or overloaded.
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// Transient marks err as retryable.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &TransientError{Err: err}
}

// IsTransient reports whether err is retryable.
func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te)
}
