// Package bootstrap seeds a pipeline's cursor and loads the current state:
//
//   - Run (`worker bootstrap`): loads the state tables from the latest
//     history-archive checkpoint C (bucket list verified against the
//     header), seeds the explorer cursor at (C, hash_C) in the same
//     transaction, and writes state/bootstrap.json (origin.go) if it is
//     absent;
//   - Force (`--force`): state repair with ingest stopped; the cursor only
//     moves forward and no ledger row is skipped;
//   - HistoryOnly (`--history-only --at A`, dev/test): seeds only the
//     cursor at A−1.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// HistoryOnlyOptions configure HistoryOnly.
type HistoryOnlyOptions struct {
	// DSN connects as ingest_writer.
	DSN string
	// At is the first ledger to ingest.
	At uint32
	// Sources are tried in order (lake, archive, AWS lake) to read ledger
	// At's previous hash.
	Sources []sequencer.Source
	// LakeDir, when set, gets state/bootstrap.json (archive_origin = At) if
	// it is absent, so the archiver and the launcher work on a dev lake too.
	LakeDir string
	Log     *slog.Logger
}

// Result describes the seeded cursor.
type Result struct {
	Cursor uint32   // At − 1
	Hash   xdr.Hash // PreviousLedgerHash(At)
	Source string   // where ledger At was read
	// Origin is state/bootstrap.json (with LakeDir); OriginWritten tells
	// whether this run created it.
	Origin        Origin
	OriginWritten bool
}

// HistoryOnly seeds the explorer cursor at At−1. It refuses when the
// pipeline already has a cursor.
func HistoryOnly(ctx context.Context, o HistoryOnlyOptions) (Result, error) {
	if o.At < 2 {
		return Result{}, errors.New("--at must be a ledger sequence ≥ 2")
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	conn, err := pgx.Connect(ctx, o.DSN)
	if err != nil {
		return Result{}, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if err := explorer.CheckSchema(ctx, conn); err != nil {
		return Result{}, err
	}
	if seq, _, err := explorer.ReadCursor(ctx, conn, explorer.Pipeline); err == nil {
		return Result{}, fmt.Errorf("%w %q (cursor at %d): bootstrap runs once", explorer.ErrCursorExists, explorer.Pipeline, seq)
	} else if !errors.Is(err, explorer.ErrNoCursor) {
		return Result{}, err
	}

	h, source, err := readHeader(ctx, o.Sources, o.At, log)
	if err != nil {
		return Result{}, err
	}
	r := Result{Cursor: o.At - 1, Hash: h.PreviousHash, Source: source}
	if err := explorer.SeedCursor(ctx, conn, explorer.Pipeline, r.Cursor, r.Hash); err != nil {
		return Result{}, err
	}
	if o.LakeDir != "" {
		origin := NewOrigin(network.PublicNetworkPassphrase, r.Cursor, r.Hash)
		written, existing, err := WriteOriginIfAbsent(o.LakeDir, origin)
		if err != nil {
			return r, fmt.Errorf("cursor seeded, but %s: %w", OriginFile(o.LakeDir), err)
		}
		r.Origin, r.OriginWritten = origin, written
		if !written {
			r.Origin = existing
		}
	}
	return r, nil
}

// readHeader reads and fully validates ledger seq from the first source
// that has it.
func readHeader(ctx context.Context, sources []sequencer.Source, seq uint32, log *slog.Logger) (sep54.Header, string, error) {
	var errs []error
	for _, src := range sources {
		obj, err := src.GetLedger(ctx, seq)
		if err == nil {
			var h sep54.Header
			if _, h, err = sep54.DecodeLedger(seq, obj.Body); err == nil {
				return h, src.Name(), nil
			}
		}
		if ctx.Err() != nil {
			return sep54.Header{}, "", ctx.Err()
		}
		log.Info("ledger not readable from this source; trying the next", "ledger", seq, "source", src.Name(), "err", err)
		errs = append(errs, fmt.Errorf("%s: %w", src.Name(), err))
	}
	return sep54.Header{}, "", fmt.Errorf("ledger %d is not readable from any source: %w", seq, errors.Join(errs...))
}
