package explorer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// StateCheckpoint is the row of explorer.state_checkpoint
// (state_checkpoint.sql).
type StateCheckpoint struct {
	Checkpoint  uint32
	Hash        xdr.Hash
	LoadedAt    time.Time
	LoadSeconds float64
	Entries     map[string]int64
}

// ReadStateCheckpoint returns the checkpoint the state tables were loaded
// from; ok is false while they were never loaded.
func ReadStateCheckpoint(ctx context.Context, q querier) (sc StateCheckpoint, ok bool, err error) {
	var seq int64
	var hash, entries []byte
	err = q.QueryRow(ctx, "SELECT checkpoint, hash, loaded_at, load_seconds, entries FROM explorer.state_checkpoint").
		Scan(&seq, &hash, &sc.LoadedAt, &sc.LoadSeconds, &entries)
	if errors.Is(err, pgx.ErrNoRows) {
		return StateCheckpoint{}, false, nil
	}
	if err != nil {
		return StateCheckpoint{}, false, classify(err)
	}
	if len(hash) != len(sc.Hash) {
		return StateCheckpoint{}, false, fmt.Errorf("explorer.state_checkpoint: bad hash length %d", len(hash))
	}
	copy(sc.Hash[:], hash)
	sc.Checkpoint = uint32(seq)
	if err := json.Unmarshal(entries, &sc.Entries); err != nil {
		return StateCheckpoint{}, false, fmt.Errorf("explorer.state_checkpoint entries: %w", err)
	}
	return sc, true, nil
}

// WriteStateCheckpoint records (or replaces) the checkpoint row.
func WriteStateCheckpoint(ctx context.Context, q querier, sc StateCheckpoint) error {
	entries, err := json.Marshal(sc.Entries)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `INSERT INTO explorer.state_checkpoint (singleton, checkpoint, hash, loaded_at, load_seconds, entries)
		VALUES (true, $1, $2, now(), $3, $4)
		ON CONFLICT (singleton) DO UPDATE SET checkpoint = EXCLUDED.checkpoint, hash = EXCLUDED.hash,
			loaded_at = EXCLUDED.loaded_at, load_seconds = EXCLUDED.load_seconds, entries = EXCLUDED.entries`,
		int64(sc.Checkpoint), sc.Hash[:], sc.LoadSeconds, string(entries))
	return err
}
