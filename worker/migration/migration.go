// Package migration parses the schema files of the worker's stores (the
// explorer schema in PostgreSQL, database stellar in ClickHouse). The schema
// lives next to the code that writes it: each entity's .sql file holds that
// entity's migrations, as sections starting with a marker line
//
//	-- +migration <version> <name>
//
// Versions are global per store and contiguous (1, 2, 3, …); a later change
// to an entity appends a new section with the next free version to its
// file. Applied sections must never be edited: their checksum is recorded
// in the store and verified on every run.
package migration

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Migration is one ordered schema change.
type Migration struct {
	Version  int
	Name     string
	File     string
	SQL      string
	Checksum string // hex SHA-256 of SQL
}

var marker = regexp.MustCompile(`^-- \+migration (\d+) ([a-z0-9_]+)\s*$`)

// Parse reads every *.sql file of fsys and returns its migrations in
// version order, checking that versions run 1..n without gaps.
func Parse(fsys fs.FS) ([]Migration, error) {
	files, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	var all []Migration
	for _, file := range files {
		b, err := fs.ReadFile(fsys, file)
		if err != nil {
			return nil, err
		}
		var cur *Migration
		var body strings.Builder
		flush := func() {
			if cur != nil {
				cur.SQL = strings.TrimSpace(body.String()) + "\n"
				sum := sha256.Sum256([]byte(cur.SQL))
				cur.Checksum = hex.EncodeToString(sum[:])
				all = append(all, *cur)
			}
			body.Reset()
		}
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if m := marker.FindStringSubmatch(line); m != nil {
				flush()
				v, _ := strconv.Atoi(m[1])
				cur = &Migration{Version: v, Name: m[2], File: file}
				continue
			}
			if strings.HasPrefix(line, "-- +migration") {
				return nil, fmt.Errorf("%s: malformed migration marker %q", file, line)
			}
			if cur != nil {
				body.WriteString(line)
				body.WriteByte('\n')
			}
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		flush()
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Version < all[j].Version })
	for i, m := range all {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migrations must be numbered 1..%d without gaps or duplicates: found %d (%s, %s) at position %d",
				len(all), m.Version, m.Name, m.File, i+1)
		}
		if strings.TrimSpace(m.SQL) == "" {
			return nil, fmt.Errorf("migration %d (%s) is empty", m.Version, m.Name)
		}
	}
	return all, nil
}
