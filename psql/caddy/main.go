// Command caddy is the explorer API's Caddy: the standard Caddy v2 build plus
// the third-party rate_limit handler (github.com/mholt/caddy-ratelimit),
// which the official binaries do not include.
//
// It is what `xcaddy build v2.11.4 --with github.com/mholt/caddy-ratelimit@5625512f24f6`
// generates, except that the versions are pinned in go.mod and go.sum and
// reviewed like every other dependency. `make dist` at the repository root
// builds it for the server, and lab psql installs the file next to itself
// (step caddy).
package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	// Everything the official binaries contain.
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	// http.handlers.rate_limit: the per-client-IP zone in files/Caddyfile.
	_ "github.com/mholt/caddy-ratelimit"
)

func main() {
	caddycmd.Main()
}
