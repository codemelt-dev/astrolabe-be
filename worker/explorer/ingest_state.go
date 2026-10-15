package explorer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ReadCursor returns a pipeline's cursor from public.ingest_state
// (ingest_state.sql), or an error wrapping ErrNoCursor.
func ReadCursor(ctx context.Context, q querier, pipeline string) (uint32, xdr.Hash, error) {
	var seq int64
	var hashHex string
	err := q.QueryRow(ctx, "SELECT ledger_sequence, ledger_hash FROM public.ingest_state WHERE pipeline = $1", pipeline).
		Scan(&seq, &hashHex)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, xdr.Hash{}, fmt.Errorf("%w %q", ErrNoCursor, pipeline)
	}
	if err != nil {
		return 0, xdr.Hash{}, classify(err)
	}
	h, err := parseHash(hashHex)
	if err != nil {
		return 0, xdr.Hash{}, fmt.Errorf("ingest_state %q: %w", pipeline, err)
	}
	return uint32(seq), h, nil
}

// ErrCursorExists means a pipeline already has a cursor: bootstrap runs
// once.
var ErrCursorExists = errors.New("ingest_state already has a row for this pipeline")

// SeedCursor inserts a pipeline's first cursor: the ledger before the first
// one to ingest and that ledger's hash (the next ledger's
// PreviousLedgerHash).
func SeedCursor(ctx context.Context, q querier, pipeline string, seq uint32, hash xdr.Hash) error {
	tag, err := q.Exec(ctx, `INSERT INTO public.ingest_state (pipeline, ledger_sequence, ledger_hash) VALUES ($1, $2, $3)
		ON CONFLICT (pipeline) DO NOTHING`, pipeline, int64(seq), hex.EncodeToString(hash[:]))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w %q", ErrCursorExists, pipeline)
	}
	return nil
}

// DeleteCursor removes a pipeline's row (a finished reindex that runs
// again).
func DeleteCursor(ctx context.Context, q querier, pipeline string) error {
	_, err := q.Exec(ctx, "DELETE FROM public.ingest_state WHERE pipeline = $1", pipeline)
	return err
}

// LedgerHash returns the hash stored in explorer.ledgers for seq, if any.
func LedgerHash(ctx context.Context, q querier, seq uint32) (xdr.Hash, bool, error) {
	var b []byte
	err := q.QueryRow(ctx, "SELECT hash FROM explorer.ledgers WHERE sequence = $1", int64(seq)).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return xdr.Hash{}, false, nil
	}
	if err != nil {
		return xdr.Hash{}, false, err
	}
	var h xdr.Hash
	if len(b) != len(h) {
		return h, false, fmt.Errorf("explorer.ledgers %d: bad hash length %d", seq, len(b))
	}
	copy(h[:], b)
	return h, true, nil
}
