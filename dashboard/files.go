// Package dashboard embeds the Grafana configuration and dashboards for the
// lab CLI (../cli), which installs them on the psql host (`lab grafana`).
package dashboard

import "embed"

//go:embed grafana.ini grafana-public.conf datasources.yaml clickhouse-datasource.yaml dashboards.yaml *.json
var Files embed.FS
