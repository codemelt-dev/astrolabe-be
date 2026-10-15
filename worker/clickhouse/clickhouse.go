// Package clickhouse is the clickhouse pipeline (clickhouse/README.md): the
// decoded history, written to ClickHouse on the clickhouse host. It runs
// the explorer package's processLedger (explorer.HistoryProcessor) in its
// own sequencer, reads ledgers from the archive bucket (the AWS public lake
// as the fallback), and keeps its cursor in ClickHouse.
//
// ClickHouse has no transaction across tables, so a batch is written table
// by table and the cursor last; rows above the cursor are never trusted and
// are deleted before the next batch after a start or a failed commit
// (Sink.cleanAbove). The schema lives next to this code, in the .sql files,
// in package migration's format.
package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sequencer"
)

// Pipeline is this pipeline's name in ingest_state and the metrics.
const Pipeline = "clickhouse"

// Open connects to ClickHouse as the writer (ingest, init, check), or as
// the schema owner when owner is true (migrate).
func Open(ctx context.Context, c config.ClickHouse, owner bool) (driver.Conn, error) {
	user, file := c.User, c.PasswordFile
	if owner {
		user, file = c.MigrateUser, c.MigratePasswordFile
	}
	pw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("clickhouse password: %w", err)
	}
	opts := &ch.Options{
		Addr:        []string{c.Addr},
		Auth:        ch.Auth{Database: c.Database, Username: user, Password: strings.TrimSpace(string(pw))},
		DialTimeout: 5 * time.Second,
		// One connection for the sink and its queries; a second for the
		// metrics' ping while a batch is being sent.
		MaxOpenConns: 2,
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("clickhouse CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("clickhouse CA %s: no certificate", c.CAFile)
		}
		opts.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	conn, err := ch.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("connect to clickhouse: %w", err)
	}
	return conn, nil
}

// deterministic are ClickHouse error codes that retrying the same rows
// cannot fix: a schema, data or permission error halts the pipeline.
var deterministic = map[int32]bool{
	6:   true, // CANNOT_PARSE_TEXT
	16:  true, // NO_SUCH_COLUMN_IN_TABLE
	27:  true, // CANNOT_PARSE_INPUT_ASSERTION_FAILED
	47:  true, // UNKNOWN_IDENTIFIER
	53:  true, // TYPE_MISMATCH
	60:  true, // UNKNOWN_TABLE
	62:  true, // SYNTAX_ERROR
	70:  true, // CANNOT_CONVERT_TYPE
	81:  true, // UNKNOWN_DATABASE
	497: true, // ACCESS_DENIED
	516: true, // AUTHENTICATION_FAILED
}

// classify marks errors as the sequencer expects: deterministic server
// errors halt at ledger seq; everything else (connections, timeouts,
// overload, a restarting server) is transient.
func classify(seq uint32, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || sequencer.IsTransient(err) || sequencer.IsHalt(err) {
		return err
	}
	var exc *ch.Exception
	if errors.As(err, &exc) && deterministic[exc.Code] {
		return sequencer.Halt(seq, err)
	}
	return sequencer.Transient(err)
}
