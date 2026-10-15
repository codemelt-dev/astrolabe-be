package main

// The step engine. Every step has a read-only check and an apply that runs
// only when the check is not ok. `run` checks everything, asks every question
// once, then applies in order without further prompts; `check` stops after the
// checks and the health probes.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"charm.land/huh/v2"
	"charm.land/huh/v2/spinner"
	"charm.land/lipgloss/v2"
)

type status int

const (
	statusOK      status = iota
	statusChange         // not in the documented state; apply fixes it
	statusWaiting        // depends on something outside this host
	statusRestart        // configured, but a running service needs a restart
	statusFailed
)

type result struct {
	status status
	detail string
	ask    string // a question that needs a yes before apply may act (formatting a disk, a long job)
}

func ok(format string, a ...any) result {
	return result{status: statusOK, detail: fmt.Sprintf(format, a...)}
}
func change(format string, a ...any) result {
	return result{status: statusChange, detail: fmt.Sprintf(format, a...)}
}
func waiting(format string, a ...any) result {
	return result{status: statusWaiting, detail: fmt.Sprintf(format, a...)}
}
func restart(format string, a ...any) result {
	return result{status: statusRestart, detail: fmt.Sprintf(format, a...)}
}
func failed(format string, a ...any) result {
	return result{status: statusFailed, detail: fmt.Sprintf(format, a...)}
}

type step struct {
	name   string
	about  string
	check  func(*env) result // read-only
	apply  func(*env) error  // only when check is not ok; safe to repeat
	health func(*env) result // optional, read-only: what an operator would look at
}

// env is what steps get: the run's context, settings and switches, the log,
// and the notes an apply leaves for its result line.
type env struct {
	ctx          context.Context
	set          settings
	allowRestart bool
	approved     map[string]bool // step name → its question was answered yes
	log          io.Writer
	notes        []string
	aptUpdated   bool
}

// note records what an apply changed; the step's result line shows it.
func (e *env) note(format string, a ...any) { e.notes = append(e.notes, fmt.Sprintf(format, a...)) }

var errNotApproved = errors.New("not approved")

// host is one server this CLI sets up: its folder's files (embedded), its
// settings and its step table.
type host struct {
	name, short string
	files       fs.FS
	settings    []setting
	steps       func() []step
}

// current is the host this run sets up; one process sets up one host.
var current *host

// tool names this run in logs, headers and markers: "lab psql".
func tool() string { return app + " " + current.name }

func settingsPath(h *host) string { return "/etc/stellar/" + app + "-" + h.name + ".toml" }

func execute(ctx context.Context, h *host, o options, apply bool) error {
	current = h
	ctx, cancel := context.WithCancel(ctx) // spin cancels it on Ctrl+C
	defer cancel()
	if err := preflight(); err != nil {
		return err
	}
	for _, other := range hosts {
		if other != h && exists(settingsPath(other)) {
			return fmt.Errorf("this machine is the %s host (%s exists)", other.name, settingsPath(other))
		}
	}
	if o.config == "" {
		o.config = settingsPath(h)
	}
	unlock, err := lock()
	if err != nil {
		return err
	}
	defer unlock()
	logf, err := openLog()
	if err != nil {
		return err
	}
	defer logf.Close()
	fmt.Fprintf(logf, "\n=== %s %s %s %s\n", time.Now().UTC().Format(time.RFC3339), tool(), version, strings.Join(os.Args[1:], " "))

	steps, err := pick(h.steps(), o.only, o.skip)
	if err != nil {
		return usageError{err}
	}
	// `check` leaves missing settings empty: report them before a step
	// renders its files or probes with them.
	for i, s := range steps {
		unset := func(e *env) string {
			for _, d := range h.settings {
				if e.set[d.key] == "" && d.needed(e.set, []step{s}) {
					return d.key
				}
			}
			return ""
		}
		steps[i].check = func(e *env) result {
			if k := unset(e); k != "" {
				return change("setting %s is missing", k)
			}
			return s.check(e)
		}
		if s.health != nil {
			steps[i].health = func(e *env) result {
				if k := unset(e); k != "" {
					return failed("setting %s is missing", k)
				}
				return s.health(e)
			}
		}
	}
	interactive := apply && !o.yes
	if interactive && !isTerminal() {
		return usageError{errors.New("not a terminal: pass --yes to run without prompts")}
	}
	set, err := loadSettings(o.config, steps, interactive, apply)
	if err != nil {
		return err
	}
	e := &env{ctx: ctx, set: set, allowRestart: o.allowRestart, approved: map[string]bool{}, log: logf}

	results := make([]result, len(steps))
	for i, s := range steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		results[i] = s.check(e)
	}
	if !apply {
		return report(e, steps, results)
	}

	// A pending restart is applied only with --allow-restart; without it the
	// step stays "!" and the run fails.
	due := func(r result) bool { return r.status == statusChange || r.status == statusRestart && o.allowRestart }
	var todo, asks []string
	for i, s := range steps {
		if r := results[i]; due(r) {
			todo = append(todo, s.name)
			if r.ask != "" {
				asks = append(asks, s.name)
			}
		}
	}
	if len(todo) == 0 {
		printTable(steps, results)
		return finish(e, steps, results)
	}
	if interactive {
		printTable(steps, results)
		if err := confirm(e, steps, results, todo, asks); err != nil {
			return err
		}
	} else {
		for _, name := range asks {
			e.approved[name] = true
		}
	}

	for i, s := range steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		r := s.check(e) // again: earlier steps change what later ones see
		if due(r) {
			e.notes = nil
			err := spin(ctx, cancel, s.name, func() error { return s.apply(e) })
			switch {
			case errors.Is(err, errNotApproved):
				r = failed("needs approval: %s", r.ask)
			case err != nil:
				r = failed("%v (log: %s)", err, logPath())
			default:
				after := s.check(e)
				if after.status == statusChange {
					after = failed("still not ok after apply: %s", after.detail)
				}
				if len(e.notes) > 0 && after.status == statusOK {
					after.detail = strings.Join(e.notes, "; ")
					after.status = statusChange // shown as ↻: changed by this run
				}
				r = after
			}
		}
		results[i] = r
		printLine(s.name, r, true)
		if r.status == statusFailed {
			return fmt.Errorf("step %s failed", s.name)
		}
	}
	return finish(e, steps, results)
}

// finish prints the health probes after a run and fails if a step failed or
// a restart is still pending. Health doesn't fail a run: right after a first
// setup the pipeline is still catching up.
func finish(e *env, steps []step, results []result) error {
	printHealth(e, steps)
	var pending []string
	for i, r := range results {
		switch r.status {
		case statusFailed:
			return fmt.Errorf("step %s failed", steps[i].name)
		case statusRestart:
			pending = append(pending, steps[i].name)
		}
	}
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if len(pending) > 0 {
		return fmt.Errorf("restart pending (%s): run again with --allow-restart", strings.Join(pending, ", "))
	}
	return nil
}

// report is `check`: the step table, the health probes, and an error unless
// everything is ok.
func report(e *env, steps []step, results []result) error {
	printTable(steps, results)
	bad := 0
	for _, r := range results {
		if r.status != statusOK {
			bad++
		}
	}
	bad += printHealth(e, steps)
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if bad > 0 {
		return fmt.Errorf("%d not ok", bad)
	}
	return nil
}

func confirm(e *env, steps []step, results []result, todo, asks []string) error {
	yes := true
	fields := []huh.Field{huh.NewConfirm().
		Title(fmt.Sprintf("Apply %d step(s): %s?", len(todo), strings.Join(todo, ", "))).
		Value(&yes)}
	answers := map[string]*bool{}
	for i, s := range steps {
		if slices.Contains(asks, s.name) {
			a := false
			answers[s.name] = &a
			fields = append(fields, huh.NewConfirm().Title(s.name+": "+results[i].ask).Value(&a))
		}
	}
	if err := huh.NewForm(huh.NewGroup(fields...)).Run(); err != nil {
		return err
	}
	if !yes {
		return errors.New("aborted, nothing changed")
	}
	for name, a := range answers {
		e.approved[name] = *a
	}
	return nil
}

// spin runs fn under a spinner on a terminal, plainly otherwise. Ctrl+C
// under the spinner cancels the run's commands, and spin returns only once
// fn has.
func spin(ctx context.Context, cancel context.CancelFunc, title string, fn func() error) error {
	if !isTerminal() {
		return fn()
	}
	var err error
	done := make(chan struct{})
	go func() { err = fn(); close(done) }()
	if serr := spinner.New().Context(ctx).Title(" " + title).Action(func() { <-done }).Run(); serr != nil {
		cancel()
		<-done
		return serr
	}
	return err
}

// Output

var (
	styleName = lipgloss.NewStyle().Width(12)
	styleDim  = lipgloss.NewStyle().Faint(true)
	glyphs    = map[status]string{
		statusOK:      lipgloss.NewStyle().Foreground(lipgloss.Green).Render("✓"),
		statusChange:  lipgloss.NewStyle().Foreground(lipgloss.Blue).Render("↻"),
		statusWaiting: lipgloss.NewStyle().Foreground(lipgloss.Yellow).Render("◷"),
		statusRestart: lipgloss.NewStyle().Foreground(lipgloss.Magenta).Render("!"),
		statusFailed:  lipgloss.NewStyle().Foreground(lipgloss.Red).Render("✗"),
	}
)

func printTable(steps []step, results []result) {
	for i, s := range steps {
		printLine(s.name, results[i], false)
	}
}

// printLine prints one step. Before apply, "change" reads as "will change".
func printLine(name string, r result, applied bool) {
	g := glyphs[r.status]
	if r.status == statusChange && !applied {
		g = lipgloss.NewStyle().Foreground(lipgloss.Blue).Render("•")
	}
	lipgloss.Println(" " + g + " " + styleName.Render(name) + styleDim.Render(r.detail))
}

// printHealth runs the steps' health probes and returns how many aren't ok.
func printHealth(e *env, steps []step) int {
	bad, header := 0, false
	for _, s := range steps {
		if s.health == nil || e.ctx.Err() != nil {
			continue
		}
		if !header {
			lipgloss.Println(styleDim.Render("\n health"))
			header = true
		}
		r := s.health(e)
		if r.status != statusOK {
			bad++
		}
		printLine(s.name, r, true)
	}
	return bad
}

// Run-wide guards

func preflight() error {
	if os.Geteuid() != 0 {
		return errors.New("run as root")
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return fmt.Errorf("needs linux/amd64, this is %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	osr, _ := os.ReadFile("/etc/os-release")
	if !strings.Contains(string(osr), "\nID=ubuntu") || !strings.Contains(string(osr), `VERSION_ID="24.04"`) {
		return errors.New("needs Ubuntu 24.04")
	}
	return nil
}

// lock keeps two runs from interleaving on one host.
func lock() (func(), error) {
	f, err := os.OpenFile("/run/lock/"+app+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("another " + app + " is running")
	}
	return func() { f.Close() }, nil
}

func logPath() string { return "/var/log/stellar-setup/" + current.name + ".log" }

func openLog() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath()), 0o700); err != nil {
		return nil, err
	}
	return os.OpenFile(logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
}

func isTerminal() bool {
	for _, f := range []*os.File{os.Stdin, os.Stdout} {
		if fi, err := f.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
			return false
		}
	}
	return true
}

// pick applies --only and --skip, keeping the table's order.
func pick(all []step, only, skip []string) ([]step, error) {
	names := map[string]bool{}
	for _, s := range all {
		names[s.name] = true
	}
	for _, n := range append(slices.Clone(only), skip...) {
		if !names[n] {
			return nil, fmt.Errorf("unknown step %q", n)
		}
	}
	var out []step
	for _, s := range all {
		if (len(only) == 0 || slices.Contains(only, s.name)) && !slices.Contains(skip, s.name) {
			out = append(out, s)
		}
	}
	return out, nil
}
