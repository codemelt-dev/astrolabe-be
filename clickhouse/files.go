// Package clickhouse embeds this folder's configuration files for the lab
// CLI (../cli), which installs them on the clickhouse host (README.md).
package clickhouse

import "embed"

//go:embed files
var Files embed.FS
