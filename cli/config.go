package main

// Settings, secrets, and rendering the repository's files with them.
//
// The host folders' files are written with their README's example values, so
// they read correctly as they are. Installing one replaces each example value
// by the configured value, whole tokens only (`stellar-ledgers-pubnet` does
// not touch `stellar-ledgers-pubnet-staging`).

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"charm.land/huh/v2"
	"github.com/BurntSushi/toml"
)

// setting is one value the operator chooses. Secrets are kept as files in
// secretsDir and never in the settings file.
type setting struct {
	key     string
	title   string
	example string               // the literal the repository files use; "" if none
	def     string               // default; "" means it must be given
	secret  bool                 // entered once, stored in secretsDir/<key>
	steps   []string             // asked only when one of these steps is selected
	when    func(settings) bool  // asked only when this is true (nil: always)
	valid   func(v string) error // nil: anything non-empty
}

type settings map[string]string

const secretsDir = "/etc/stellar/secrets"

// get returns a setting's value; asking for an undeclared key is a bug.
func (s settings) get(key string) string {
	if _, known := s[key]; !known {
		panic("unknown setting " + key)
	}
	return s[key]
}

// loadSettings reads the settings file and the entered secrets. For `run` it
// asks for what is missing (or fails listing it with --yes) and saves the
// answers; `check` leaves missing values empty for the steps to report.
func loadSettings(path string, steps []step, interactive, apply bool) (settings, error) {
	set := settings{}
	for _, d := range current.settings {
		set[d.key] = d.def
	}
	saved := map[string]string{}
	if _, err := toml.DecodeFile(path, &saved); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for k, v := range saved {
		i := slices.IndexFunc(current.settings, func(d setting) bool { return d.key == k })
		if i < 0 {
			return nil, fmt.Errorf("%s: unknown setting %q", path, k)
		} else if d := current.settings[i]; d.secret {
			return nil, fmt.Errorf("%s: %s is a secret: remove it, and keep it in %s/%[2]s", path, k, secretsDir)
		} else if err := d.check(v); err != nil {
			return nil, fmt.Errorf("%s: %s: %w", path, k, err)
		}
		set[k] = strings.TrimSpace(v)
	}
	for _, d := range current.settings {
		if d.secret {
			if b, err := os.ReadFile(filepath.Join(secretsDir, d.key)); err == nil {
				set[d.key] = strings.TrimSpace(string(b))
			}
		}
	}
	if !apply {
		return set, nil
	}

	var todo []setting
	for _, d := range current.settings {
		if set[d.key] == "" && d.needed(set, steps) {
			todo = append(todo, d)
		}
	}
	if len(todo) == 0 {
		return set, nil
	}
	if !interactive {
		var keys []string
		for _, d := range todo {
			keys = append(keys, d.key)
		}
		return nil, fmt.Errorf("missing settings in %s: %s", path, strings.Join(keys, ", "))
	}
	values := make([]string, len(todo))
	var fields []huh.Field
	for i, d := range todo {
		in := huh.NewInput().Title(d.title).Value(&values[i]).Validate(d.check)
		if d.example != "" {
			in = in.Placeholder(d.example)
		}
		if d.secret {
			in = in.EchoMode(huh.EchoModePassword)
		}
		fields = append(fields, in)
	}
	if err := huh.NewForm(huh.NewGroup(fields...).Title("Settings for " + tool())).Run(); err != nil {
		return nil, err
	}
	for i, d := range todo {
		set[d.key] = strings.TrimSpace(values[i])
		if d.secret {
			if err := writeSecret(d.key, set[d.key]); err != nil {
				return nil, err
			}
		} else {
			saved[d.key] = set[d.key]
		}
	}
	return set, saveSettings(path, saved)
}

func (d setting) needed(set settings, steps []step) bool {
	if d.when != nil && !d.when(set) {
		return false
	}
	if len(d.steps) == 0 {
		return true
	}
	for _, s := range steps {
		if slices.Contains(d.steps, s.name) {
			return true
		}
	}
	return false
}

func (d setting) check(v string) error {
	v = strings.TrimSpace(v)
	if v == "" {
		return errors.New("required")
	}
	if d.valid != nil {
		return d.valid(v)
	}
	return nil
}

func saveSettings(path string, saved map[string]string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s settings (%s/README.md). Secrets are in %s, not here.\n", tool(), current.name, secretsDir)
	keys := make([]string, 0, len(saved))
	for k := range saved {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %q\n", k, saved[k])
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

// asset returns one of the host folder's files, rendered with the settings.
func (e *env) asset(name string) []byte {
	b, err := fs.ReadFile(current.files, name)
	if err != nil {
		panic(err) // embedded at build time: a missing name is a bug
	}
	return e.set.render(b)
}

func isIP(v string) error {
	if _, err := netip.ParseAddr(v); err != nil {
		return errors.New("not an IP address")
	}
	return nil
}

// matching validates a setting against a regular expression (whole value).
func matching(re, msg string) func(string) error {
	r := regexp.MustCompile(`^(?:` + re + `)$`)
	return func(v string) error {
		if !r.MatchString(v) {
			return errors.New(msg)
		}
		return nil
	}
}

// Secrets

func writeSecret(name, value string) error {
	// MkdirAll gives every directory it creates the same mode: /etc/stellar first.
	if err := os.MkdirAll(filepath.Dir(secretsDir), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(secretsDir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(secretsDir, name), []byte(value+"\n"), 0o600)
}

// generated returns a secret this tool generates once (32 random bytes,
// hex) and keeps in secretsDir; it is never rotated silently.
func generated(name string) (string, error) {
	if b, err := os.ReadFile(filepath.Join(secretsDir, name)); err == nil {
		return strings.TrimSpace(string(b)), nil
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	v := hex.EncodeToString(buf)
	return v, writeSecret(name, v)
}

// Rendering

// render replaces every example value in data with its configured value, in
// one pass: a configured value that equals another example (this host's IP
// is the example galexie IP) is never replaced again.
func (s settings) render(data []byte) []byte {
	var pairs [][2]string
	for _, d := range current.settings {
		if d.example != "" && !d.secret && s[d.key] != "" {
			pairs = append(pairs, [2]string{d.example, s[d.key]})
		}
	}
	// Longest first, so an example that contains another wins.
	sort.SliceStable(pairs, func(i, j int) bool { return len(pairs[i][0]) > len(pairs[j][0]) })
	return []byte(replaceTokens(string(data), pairs))
}

func replaceToken(s, old, new string) string { return replaceTokens(s, [][2]string{{old, new}}) }

// replaceTokens replaces each old by its new where old is a whole token: not
// preceded or followed by a letter, digit, '_' or '-', nor followed by '.'
// and one of those (so 10.0.0.2 doesn't match inside 10.0.0.20 or 10.0.0.2.5).
func replaceTokens(s string, pairs [][2]string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		n := 0
		for _, p := range pairs {
			end := i + len(p[0])
			if strings.HasPrefix(s[i:], p[0]) && (i == 0 || !word(s[i-1])) &&
				(end == len(s) || !word(s[end]) && (s[end] != '.' || end+1 == len(s) || !word(s[end+1]))) {
				b.WriteString(p[1])
				n = len(p[0])
				break
			}
		}
		if n == 0 {
			b.WriteByte(s[i])
			n = 1
		}
		i += n
	}
	return b.String()
}

func word(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}
