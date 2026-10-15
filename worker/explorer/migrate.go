package explorer

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"

	"stellar-explorer/worker/migration"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The schema lives next to the code that writes it, in this package's .sql
// files, in the format of package migration. Applied sections must never be
// edited: their checksum is recorded in explorer.schema_migrations and
// verified on every run.
//
//go:embed *.sql
var sqlFiles embed.FS

// Migration is one ordered schema change.
type Migration = migration.Migration

// Migrations returns the embedded migrations in version order.
func Migrations() ([]Migration, error) {
	return parseMigrations(sqlFiles)
}

func parseMigrations(fsys fs.FS) ([]Migration, error) { return migration.Parse(fsys) }

// migrationLockKey serialises concurrent `worker migrate` runs.
const migrationLockKey = "explorer.migrate"

const createMigrationsTable = `
CREATE SCHEMA IF NOT EXISTS explorer;
CREATE TABLE IF NOT EXISTS explorer.schema_migrations (
    version    INTEGER     PRIMARY KEY,
    name       TEXT        NOT NULL,
    checksum   TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Migrate applies every pending migration in order, each in its own
// transaction, as the connected role (explorer_owner, which then owns every
// object). It returns the migrations it applied.
func Migrate(ctx context.Context, conn *pgx.Conn, log *slog.Logger) ([]Migration, error) {
	all, err := Migrations()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", migrationLockKey); err != nil {
		return nil, fmt.Errorf("take the migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(hashtext($1))", migrationLockKey)
	}()
	if _, err := conn.Exec(ctx, createMigrationsTable); err != nil {
		return nil, fmt.Errorf("create explorer.schema_migrations: %w", err)
	}
	applied, err := appliedMigrations(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := verifyApplied(all, applied); err != nil {
		return nil, err
	}
	var done []Migration
	for _, m := range all {
		if _, ok := applied[m.Version]; ok {
			continue
		}
		log.Info("applying migration", "version", m.Version, "name", m.Name, "file", m.File)
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '60s'"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO explorer.schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
				m.Version, m.Name, m.Checksum)
			return err
		})
		if err != nil {
			return done, fmt.Errorf("migration %d (%s, %s): %w", m.Version, m.Name, m.File, err)
		}
		done = append(done, m)
	}
	return done, nil
}

type appliedMigration struct {
	name, checksum string
}

func appliedMigrations(ctx context.Context, q querier) (map[int]appliedMigration, error) {
	rows, err := q.Query(ctx, "SELECT version, name, checksum FROM explorer.schema_migrations")
	if err != nil {
		return nil, err
	}
	out := map[int]appliedMigration{}
	for rows.Next() {
		var v int
		var a appliedMigration
		if err := rows.Scan(&v, &a.name, &a.checksum); err != nil {
			return nil, err
		}
		out[v] = a
	}
	return out, rows.Err()
}

// verifyApplied checks that every applied migration this binary knows is
// byte-identical to the embedded one.
func verifyApplied(all []Migration, applied map[int]appliedMigration) error {
	for _, m := range all {
		a, ok := applied[m.Version]
		if ok && a.checksum != m.Checksum {
			return fmt.Errorf("%w: migration %d (%s) was applied with different SQL (checksum %s, this build has %s): never edit an applied migration; add a new one",
				ErrSchemaMismatch, m.Version, m.Name, a.checksum, m.Checksum)
		}
	}
	return nil
}

// ErrSchemaMismatch means an applied migration differs from this build's.
var ErrSchemaMismatch = errors.New("database schema does not match this build")

// ErrSchemaBehind means the database lacks migrations this binary needs:
// run `worker migrate` (until then ingest refuses to commit and alerts).
var ErrSchemaBehind = errors.New("database schema is behind this build")

// Pending returns the embedded migrations the database lacks, in order,
// without changing anything: on an empty database that is all of them. An
// applied migration whose SQL differs from this build's is an error.
// Newer migrations applied by a newer build are fine.
func Pending(ctx context.Context, q querier) ([]Migration, error) {
	all, err := Migrations()
	if err != nil {
		return nil, err
	}
	applied, err := appliedMigrations(ctx, q)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000") { // undefined table / schema
			return all, nil
		}
		return nil, err
	}
	if err := verifyApplied(all, applied); err != nil {
		return nil, err
	}
	var pending []Migration
	for _, m := range all {
		if _, ok := applied[m.Version]; !ok {
			pending = append(pending, m)
		}
	}
	return pending, nil
}

// CheckSchema verifies that every embedded migration is applied with the
// same SQL.
func CheckSchema(ctx context.Context, q querier) error {
	pending, err := Pending(ctx, q)
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		missing := make([]string, len(pending))
		for i, m := range pending {
			missing[i] = fmt.Sprintf("%d %s", m.Version, m.Name)
		}
		return fmt.Errorf("%w: missing migrations %s", ErrSchemaBehind, strings.Join(missing, ", "))
	}
	return nil
}

// querier is satisfied by *pgx.Conn, *pgxpool.Pool and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
