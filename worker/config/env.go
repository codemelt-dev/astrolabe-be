package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
)

// GatewayCredentials are the embedded gateway's root keys, read from
// gateway.env. Galexie's container reads the same file with
// `docker --env-file`.
type GatewayCredentials struct {
	AccessKey string
	SecretKey string
}

// String redacts the secret so credentials can never leak into logs.
func (c GatewayCredentials) String() string {
	return fmt.Sprintf("{AccessKey:%s SecretKey:<redacted>}", c.AccessKey)
}

// LogValue redacts the secret in structured logs.
func (c GatewayCredentials) LogValue() slog.Value {
	return slog.GroupValue(slog.String("access_key", c.AccessKey), slog.String("secret_key", "<redacted>"))
}

// ArchiveCredentials locate the archive bucket's endpoint and hold the
// uploader key, read from archive.env.
type ArchiveCredentials struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
}

// String redacts the secret.
func (c ArchiveCredentials) String() string {
	return fmt.Sprintf("{Endpoint:%s Region:%s AccessKey:%s SecretKey:<redacted>}", c.Endpoint, c.Region, c.AccessKey)
}

// LogValue redacts the secret in structured logs.
func (c ArchiveCredentials) LogValue() slog.Value {
	return slog.GroupValue(slog.String("endpoint", c.Endpoint), slog.String("region", c.Region),
		slog.String("access_key", c.AccessKey), slog.String("secret_key", "<redacted>"))
}

// LoadGatewayEnv parses gateway.env. It must contain exactly
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, plus optionally
// AWS_EC2_METADATA_DISABLED=true.
func LoadGatewayEnv(path string) (GatewayCredentials, error) {
	vars, err := parseEnvFile(path, map[string]bool{
		"AWS_ACCESS_KEY_ID":         true,
		"AWS_SECRET_ACCESS_KEY":     true,
		"AWS_EC2_METADATA_DISABLED": false,
	})
	if err != nil {
		return GatewayCredentials{}, err
	}
	if v, ok := vars["AWS_EC2_METADATA_DISABLED"]; ok && v != "true" {
		return GatewayCredentials{}, fmt.Errorf("%s: AWS_EC2_METADATA_DISABLED must be true", path)
	}
	return GatewayCredentials{AccessKey: vars["AWS_ACCESS_KEY_ID"], SecretKey: vars["AWS_SECRET_ACCESS_KEY"]}, nil
}

// LoadArchiveEnv parses archive.env. AWS_* names are rejected:
// that file is never used by anything that reads the AWS credential chain.
func LoadArchiveEnv(path string) (ArchiveCredentials, error) {
	vars, err := parseEnvFile(path, map[string]bool{
		"ARCHIVE_S3_ENDPOINT":   true,
		"ARCHIVE_S3_REGION":     true,
		"ARCHIVE_S3_ACCESS_KEY": true,
		"ARCHIVE_S3_SECRET_KEY": true,
	})
	if err != nil {
		return ArchiveCredentials{}, err
	}
	c := ArchiveCredentials{
		Endpoint:  vars["ARCHIVE_S3_ENDPOINT"],
		Region:    vars["ARCHIVE_S3_REGION"],
		AccessKey: vars["ARCHIVE_S3_ACCESS_KEY"],
		SecretKey: vars["ARCHIVE_S3_SECRET_KEY"],
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return ArchiveCredentials{}, fmt.Errorf("%s: ARCHIVE_S3_ENDPOINT %q must be an http(s)://host URL", path, c.Endpoint)
	}
	return c, nil
}

var envKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// parseEnvFile reads a docker --env-file style file: KEY=VALUE lines, blank
// lines and # comments. Values are taken literally by docker, so anything
// that docker and a shell would read differently (quotes, surrounding
// whitespace) is rejected rather than guessed at. allowed maps each permitted
// key to whether it is required; unknown or duplicate keys are errors.
func parseEnvFile(path string, allowed map[string]bool) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s: not a regular file", path)
	}
	if fi.Mode().Perm()&0o007 != 0 {
		return nil, fmt.Errorf("%s: mode %04o is accessible to other users; want 0640 or stricter", path, fi.Mode().Perm())
	}

	vars := make(map[string]string)
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSuffix(sc.Text(), "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: want KEY=VALUE", path, n)
		}
		if !envKeyRe.MatchString(key) {
			return nil, fmt.Errorf("%s:%d: invalid key %q", path, n, key)
		}
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("%s:%d: unexpected key %s", path, n, key)
		}
		if _, dup := vars[key]; dup {
			return nil, fmt.Errorf("%s:%d: duplicate key %s", path, n, key)
		}
		if val == "" {
			return nil, fmt.Errorf("%s:%d: %s is empty", path, n, key)
		}
		if strings.TrimSpace(val) != val {
			return nil, fmt.Errorf("%s:%d: %s has leading or trailing whitespace", path, n, key)
		}
		if strings.ContainsAny(val[:1], `"'`) || strings.ContainsAny(val[len(val)-1:], `"'`) {
			return nil, fmt.Errorf("%s:%d: %s is quoted; env files must be unquoted (docker --env-file keeps quotes literally)", path, n, key)
		}
		vars[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var missing []string
	for k, required := range allowed {
		if _, ok := vars[k]; required && !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("%s: missing %s", path, strings.Join(missing, ", "))
	}
	return vars, nil
}
