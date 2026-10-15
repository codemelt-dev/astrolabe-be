// Package explorertest creates scratch Postgres databases (and roles) for
// tests, from TEST_DATABASE_URL. Tests skip when it is unset.
//
// Every database is named stellar_gx_<random> and every role gx_<…>_<random>;
// both are dropped when the test ends. The database behind
// TEST_DATABASE_URL is only used to issue CREATE/DROP DATABASE.
package explorertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// EnvURL names the variable holding the admin connection URL.
const EnvURL = "TEST_DATABASE_URL"

// DB is a scratch database.
type DB struct {
	Name string
	// DSN connects to the scratch database as the admin (superuser) role.
	DSN string
	// OwnerDSN and WriterDSN connect as the explorer_owner and ingest_writer
	// stand-ins created by WithRoles.
	OwnerDSN, WriterDSN string
	// Owner and Writer are the role names ("" without WithRoles).
	Owner, Writer string

	t        testing.TB
	adminURL string
}

// Options for New.
type Options struct {
	// WithRoles creates owner and writer roles set up like psql/roles.sql
	// sets up explorer_owner and ingest_writer: the owner owns the database,
	// the writer may connect and create temporary tables, and default
	// privileges give the writer DML on whatever the owner creates.
	WithRoles bool
}

// New creates a scratch database, or skips the test without
// TEST_DATABASE_URL.
func New(t testing.TB, o Options) *DB {
	t.Helper()
	admin := os.Getenv(EnvURL)
	if admin == "" {
		t.Skipf("%s is not set", EnvURL)
	}
	suffix := randomSuffix()
	db := &DB{Name: "stellar_gx_" + suffix, t: t, adminURL: admin}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect %s: %v", EnvURL, err)
	}
	defer conn.Close(ctx)

	var roleSQL []string
	if o.WithRoles {
		db.Owner, db.Writer = "gx_owner_"+suffix, "gx_writer_"+suffix
		roleSQL = []string{
			fmt.Sprintf("CREATE ROLE %s LOGIN", db.Owner),
			fmt.Sprintf("CREATE ROLE %s LOGIN", db.Writer),
		}
	}
	for _, q := range roleSQL {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	create := "CREATE DATABASE " + db.Name
	if o.WithRoles {
		create += " OWNER " + db.Owner
	}
	if _, err := conn.Exec(ctx, create); err != nil {
		t.Fatalf("%s: %v", create, err)
	}
	t.Cleanup(db.drop)

	db.DSN = withDatabase(t, admin, db.Name, "")
	if o.WithRoles {
		db.OwnerDSN = withDatabase(t, admin, db.Name, db.Owner)
		db.WriterDSN = withDatabase(t, admin, db.Name, db.Writer)
		setup := []string{
			fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC", db.Name),
			fmt.Sprintf("GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s", db.Name, db.Writer),
		}
		for _, q := range setup {
			if _, err := conn.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		db.Exec(
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s GRANT USAGE ON SCHEMAS TO %s", db.Owner, db.Writer),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", db.Owner, db.Writer),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s GRANT USAGE, SELECT ON SEQUENCES TO %s", db.Owner, db.Writer),
		)
	}
	return db
}

// PostMigrateGrants applies the writer's grant of psql/grants.sql, which
// lab psql's api step applies after `worker migrate`: TRUNCATE on the
// tables (bootstrap truncates the state tables).
func (db *DB) PostMigrateGrants() {
	db.t.Helper()
	db.Exec(fmt.Sprintf("GRANT TRUNCATE ON ALL TABLES IN SCHEMA explorer TO %s", db.Writer))
}

// Exec runs statements in the scratch database as the admin role.
func (db *DB) Exec(stmts ...string) {
	db.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, db.DSN)
	if err != nil {
		db.t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, q := range stmts {
		if _, err := conn.Exec(ctx, q); err != nil {
			db.t.Fatalf("%s: %v", q, err)
		}
	}
}

// Conn opens an admin connection to the scratch database, closed at the end
// of the test.
func (db *DB) Conn() *pgx.Conn {
	db.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, db.DSN)
	if err != nil {
		db.t.Fatal(err)
	}
	db.t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// Size returns the database's size in bytes.
func (db *DB) Size() int64 {
	db.t.Helper()
	var n int64
	if err := db.Conn().QueryRow(context.Background(), "SELECT pg_database_size(current_database())").Scan(&n); err != nil {
		db.t.Fatal(err)
	}
	return n
}

func (db *DB) drop() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, db.adminURL)
	if err != nil {
		db.t.Errorf("drop %s: %v", db.Name, err)
		return
	}
	defer conn.Close(ctx)
	stmts := []string{fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", db.Name)}
	if db.Owner != "" {
		stmts = append(stmts, "DROP ROLE IF EXISTS "+db.Owner, "DROP ROLE IF EXISTS "+db.Writer)
	}
	for _, q := range stmts {
		if _, err := conn.Exec(ctx, q); err != nil {
			db.t.Errorf("%s: %v", q, err)
		}
	}
}

// withDatabase returns the admin URL pointed at database name, optionally as
// another user (with no password: the local test server trusts it).
func withDatabase(t testing.TB, admin, name, user string) string {
	u, err := url.Parse(admin)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatalf("%s must be a postgres:// URL", EnvURL)
	}
	u.Path = "/" + name
	if user != "" {
		u.User = url.User(user)
	}
	return u.String()
}

func randomSuffix() string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
