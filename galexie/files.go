// Package galexie embeds this folder's deploy files for the lab CLI
// (../cli), which installs them on the galexie host.
package galexie

import "embed"

//go:embed deploy
var Files embed.FS
