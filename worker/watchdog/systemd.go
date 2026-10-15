package watchdog

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Systemctl is Systemd through the systemctl CLI.
type Systemctl struct {
	// Path is the systemctl binary (default /usr/bin/systemctl).
	Path string
	// Run executes a command and returns its stdout (tests); nil runs it.
	Run func(ctx context.Context, name string, args ...string) ([]byte, error)
}

func (s Systemctl) run(ctx context.Context, args ...string) ([]byte, error) {
	path := s.Path
	if path == "" {
		path = "/usr/bin/systemctl"
	}
	if s.Run != nil {
		return s.Run(ctx, path, args...)
	}
	cmd := exec.CommandContext(ctx, path, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", path, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// Unit implements Systemd with `systemctl show` (unix timestamps).
func (s Systemctl) Unit(ctx context.Context, name string) (Unit, error) {
	out, err := s.run(ctx, "show", "--timestamp=unix", "-p", "ActiveState", "-p", "ActiveEnterTimestamp", "-p", "NRestarts", name)
	if err != nil {
		return Unit{}, err
	}
	props := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok {
			props[k] = v
		}
	}
	u := Unit{State: props["ActiveState"]}
	if u.State == "" {
		return Unit{}, fmt.Errorf("systemctl show %s: no ActiveState in %q", name, out)
	}
	u.Active = u.State == "active"
	if ts := strings.TrimPrefix(props["ActiveEnterTimestamp"], "@"); ts != "" {
		sec, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return Unit{}, fmt.Errorf("systemctl show %s: ActiveEnterTimestamp %q", name, props["ActiveEnterTimestamp"])
		}
		u.Since = time.Unix(sec, 0)
	}
	if n := props["NRestarts"]; n != "" {
		if u.NRestarts, err = strconv.ParseUint(n, 10, 64); err != nil {
			return Unit{}, fmt.Errorf("systemctl show %s: NRestarts %q", name, n)
		}
	}
	return u, nil
}

// Restart implements Systemd: `systemctl restart --no-block`, so the timer
// unit does not wait for Galexie's 45 s drain.
func (s Systemctl) Restart(ctx context.Context, name string) error {
	_, err := s.run(ctx, "restart", "--no-block", name)
	return err
}
