package clickhouse

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"stellar-explorer/worker/migration"
)

//go:embed *.sql
var sqlFiles embed.FS

// Migrations returns the embedded migrations in version order.
func Migrations() ([]migration.Migration, error) { return migration.Parse(sqlFiles) }

// ErrSchemaMismatch means an applied migration differs from this build's.
var ErrSchemaMismatch = errors.New("clickhouse schema does not match this build")

const createMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations
(
    version    UInt32,
    name       String,
    checksum   String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY version`

// Migrate applies every pending migration in order, as the connected user
// (stellar_owner), and returns those it applied. ClickHouse runs DDL one
// statement at a time and without transactions, so every statement is
// idempotent (IF NOT EXISTS): a migration that failed half-way is run
// again whole. Only one migrate may run at a time (lab runs it).
func Migrate(ctx context.Context, conn driver.Conn, log *slog.Logger) ([]migration.Migration, error) {
	pending, err := Pending(ctx, conn)
	if err != nil {
		return nil, err
	}
	var done []migration.Migration
	for _, m := range pending {
		log.Info("applying migration", "version", m.Version, "name", m.Name, "file", m.File)
		for _, stmt := range Statements(m.SQL) {
			if err := conn.Exec(ctx, stmt); err != nil {
				return done, fmt.Errorf("migration %d (%s, %s): %w", m.Version, m.Name, m.File, err)
			}
		}
		if err := conn.Exec(ctx, "INSERT INTO schema_migrations (version, name, checksum) VALUES (?, ?, ?)",
			uint32(m.Version), m.Name, m.Checksum); err != nil {
			return done, fmt.Errorf("record migration %d: %w", m.Version, err)
		}
		done = append(done, m)
	}
	return done, nil
}

// Pending returns the embedded migrations the database lacks, in order,
// creating schema_migrations if needed. An applied migration whose SQL
// differs from this build's is an error; newer ones are fine.
func Pending(ctx context.Context, conn driver.Conn) ([]migration.Migration, error) {
	all, err := Migrations()
	if err != nil {
		return nil, err
	}
	if err := conn.Exec(ctx, createMigrationsTable); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM schema_migrations FINAL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	applied := map[int]string{}
	for rows.Next() {
		var v uint32
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			return nil, err
		}
		applied[int(v)] = sum
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var pending []migration.Migration
	for _, m := range all {
		sum, ok := applied[m.Version]
		switch {
		case !ok:
			pending = append(pending, m)
		case sum != m.Checksum:
			return nil, fmt.Errorf("%w: migration %d (%s) was applied with different SQL: never edit an applied migration; add a new one",
				ErrSchemaMismatch, m.Version, m.Name)
		}
	}
	return pending, nil
}

// statementEnd is a semicolon that ends a statement's last line, possibly
// followed by a comment. A ; inside a comment ends nothing.
var statementEnd = regexp.MustCompile(`;[ \t]*(--[^\n]*)?(\n|$)`)

// Statements splits a migration into its statements (ClickHouse runs one
// per request), dropping comment-only lines.
func Statements(sql string) []string {
	var out []string
	for _, part := range statementEnd.Split(sql, -1) {
		var lines []string
		for _, l := range strings.Split(part, "\n") {
			if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
				lines = append(lines, l)
			}
		}
		if len(lines) > 0 {
			out = append(out, strings.Join(lines, "\n"))
		}
	}
	return out
}
