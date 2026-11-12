package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The refreshable view daily_stats_mv recomputes only the two newest days
// (daily_stats.sql), so the days a backfill fills in are computed once, by
// `worker clickhouse-daily-stats`: the view's own query, with its window
// replaced by a range of days, one month at a time. ReplacingMergeTree
// keeps the latest computation of a day, so a range can be computed again.

const (
	dailyStatsSince  = "WITH (SELECT toDate(max(closed_at)) - 1 FROM ledgers) AS since\n"
	dailyStatsWindow = "toDate(closed_at) >= since"
	// dailyStatsWindows is how often the view's query names its window:
	// once per table it reads.
	dailyStatsWindows = 6
)

// dailyStatsQuery is the view's query for the days [from, to].
func dailyStatsQuery(from, to time.Time) (string, error) {
	raw, err := sqlFiles.ReadFile("daily_stats.sql")
	if err != nil {
		return "", err
	}
	_, query, ok := strings.Cut(string(raw), "APPEND TO daily_stats AS\n")
	if !ok || !strings.HasPrefix(query, dailyStatsSince) || strings.Count(query, dailyStatsWindow) != dailyStatsWindows {
		return "", fmt.Errorf("daily_stats.sql: the view's query has changed; adapt dailyStatsQuery to it")
	}
	query = strings.TrimSuffix(strings.TrimSpace(strings.TrimPrefix(query, dailyStatsSince)), ";")
	window := fmt.Sprintf("toDate(closed_at) BETWEEN toDate('%s') AND toDate('%s')", from.Format(time.DateOnly), to.Format(time.DateOnly))
	return "INSERT INTO daily_stats\n" + strings.ReplaceAll(query, dailyStatsWindow, window), nil
}

// RecomputeDailyStats computes daily_stats for the days [from, to] (UTC),
// one calendar month per query.
func RecomputeDailyStats(ctx context.Context, conn driver.Conn, from, to time.Time, log *slog.Logger) error {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if to.Before(from) {
		return fmt.Errorf("daily stats: %s is before %s", to.Format(time.DateOnly), from.Format(time.DateOnly))
	}
	for a := from; !a.After(to); {
		b := time.Date(a.Year(), a.Month()+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1) // the month's last day
		if b.After(to) {
			b = to
		}
		q, err := dailyStatsQuery(a, b)
		if err != nil {
			return err
		}
		start := time.Now()
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("daily stats %s to %s: %w", a.Format(time.DateOnly), b.Format(time.DateOnly), err)
		}
		log.Info("daily stats computed", "from", a.Format(time.DateOnly), "to", b.Format(time.DateOnly), "took", time.Since(start).Round(time.Second))
		a = b.AddDate(0, 0, 1)
	}
	return nil
}
