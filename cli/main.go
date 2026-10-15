// Command lab sets up the explorer's two servers and checks them:
// `lab galexie` on the galexie host (Galexie, the worker's lake and
// ingest), `lab psql` on the psql host (PostgreSQL, PostgREST, Caddy,
// pgBackRest, monitoring). Each is a setup script with a step table: every
// step checks first and changes only what is not in the state the host's
// README documents, so running it again on a set-up host changes nothing.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"charm.land/fang/v2"
	"github.com/spf13/cobra"
)

const app = "lab"

var version = "dev" // -X main.version=… (Makefile)

// hosts are the servers, in bring-up order.
var hosts = []*host{psqlHost, galexieHost, clickhouseHost}

type options struct {
	config       string
	only, skip   []string
	yes          bool
	allowRestart bool
}

// usageError makes main exit 2 instead of 1.
type usageError struct{ error }

func main() {
	var o options
	root := &cobra.Command{
		Use:   app,
		Short: "Set up the explorer's servers, or bring them back to their documented state",
		Long: `Run "lab <host>" on that host: it checks every step, applies what is
not ok, and checks again. "lab <host> check" only reports. Missing
settings are asked once and saved; secrets go to /etc/stellar/secrets.
Running services are only restarted with --allow-restart.`,
	}
	pf := root.PersistentFlags()
	pf.StringVar(&o.config, "config", "", "settings file (default /etc/stellar/lab-<host>.toml)")
	pf.StringSliceVar(&o.only, "only", nil, "only these steps (comma-separated)")
	pf.StringSliceVar(&o.skip, "skip", nil, "skip these steps (comma-separated)")
	for _, h := range hosts {
		cmd := &cobra.Command{
			Use:   h.name,
			Short: h.short,
			Long:  h.short + ".\n\nSteps:" + stepList(h),
			Args:  cobra.NoArgs,
			RunE:  func(cmd *cobra.Command, _ []string) error { return execute(cmd.Context(), h, o, true) },
		}
		cmd.Flags().BoolVarP(&o.yes, "yes", "y", false, "never prompt: approve every question, fail on missing settings")
		cmd.Flags().BoolVar(&o.allowRestart, "allow-restart", false, "restart running services whose binary or configuration changed")
		cmd.AddCommand(&cobra.Command{
			Use:   "check",
			Short: "Report every step and the host's health, changing nothing",
			Args:  cobra.NoArgs,
			RunE:  func(cmd *cobra.Command, _ []string) error { return execute(cmd.Context(), h, o, false) },
		})
		root.AddCommand(cmd)
	}
	// `lab grafana` is the psql host's grafana step on its own. Restarting
	// Grafana costs nothing, so it may restart it.
	grafana := func(apply bool) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, _ []string) error {
			wantGrafana = true
			o.only, o.skip, o.allowRestart = []string{"grafana"}, nil, true
			return execute(cmd.Context(), psqlHost, o, apply)
		}
	}
	gf := &cobra.Command{
		Use:   "grafana",
		Short: "Set up Grafana on the psql host, with the explorer's dashboards over its Prometheus",
		Long: `Installs Grafana on the psql host, on 127.0.0.1:3002, with the dashboards in the
repository's dashboard/ folder over this host's Prometheus, which scrapes both
hosts (dashboard/README.md). Needs monitoring_enabled = yes. Afterwards
"lab psql" keeps it in place as well.`,
		Args: cobra.NoArgs,
		RunE: grafana(true),
	}
	gf.Flags().BoolVarP(&o.yes, "yes", "y", false, "never prompt: approve every question, fail on missing settings")
	gf.AddCommand(&cobra.Command{
		Use:   "check",
		Short: "Report Grafana's state and health, changing nothing",
		Args:  cobra.NoArgs,
		RunE:  grafana(false),
	})
	root.AddCommand(gf)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageError{err} })

	err := fang.Execute(context.Background(), root,
		fang.WithVersion(version),
		fang.WithNotifySignal(os.Interrupt, syscall.SIGTERM))
	if err != nil {
		if errors.As(err, new(usageError)) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func stepList(h *host) (s string) {
	for _, st := range h.steps() {
		s += fmt.Sprintf("\n  %-10s %s", st.name, st.about)
	}
	return s
}
