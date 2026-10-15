// Package psql embeds this folder's SQL and config files for the lab CLI
// (../cli), which installs and applies them on the psql host.
package psql

import "embed"

//go:embed files api.sql grants.sql roles.sql
var Files embed.FS
