// Package dockerfault injects faults into real processes running as Docker
// containers.
//
// Everything here shells out to the docker CLI. That is deliberate: it works
// identically against any image, needs no client library, and the exact
// commands are printable in a report, which matters because a fault schedule
// that cannot be written down cannot be reproduced.
//
// The faults are the ones that actually stress a consensus system:
//
//	Pause      SIGSTOP the process. It is alive on the network but answers
//	           nothing, which is the classic "slow node" that heartbeat-based
//	           failure detection has to handle.
//	Kill       SIGKILL and restart. Loses everything not on disk.
//	Partition  Disconnect the container from the cluster network. The node is
//	           up and healthy and cannot reach anyone, which is what produces
//	           in-doubt operations and stale-read opportunities.
//	Isolate    Partition, plus a second network kept attached so the client
//	           can still reach the isolated node. This is the interesting one:
//	           it lets a client keep talking to a node that has lost the
//	           cluster, which is exactly where a system with weak reads
//	           returns stale data.
package dockerfault

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Run executes a docker command and returns its combined output.
func Run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	return out.String(), nil
}

// MustRun runs a docker command and ignores a failure. Used for teardown of
// things that may already be gone.
func MustRun(ctx context.Context, args ...string) {
	_, _ = Run(ctx, args...)
}

// Available reports whether a usable docker daemon is present.
func Available(ctx context.Context) error {
	_, err := Run(ctx, "version", "--format", "{{.Server.Version}}")
	return err
}

// ServerVersion returns the docker daemon version, for the record in a report.
func ServerVersion(ctx context.Context) (string, error) {
	out, err := Run(ctx, "version", "--format", "{{.Server.Version}}")
	return strings.TrimSpace(out), err
}

// Pause SIGSTOPs a container for the duration of the fault.
type Pause struct{ Container string }

func (f Pause) Name() string { return "pause(" + f.Container + ")" }
func (f Pause) Inject(ctx context.Context) error {
	_, err := Run(ctx, "pause", f.Container)
	return err
}
func (f Pause) Recover(ctx context.Context) error {
	_, err := Run(ctx, "unpause", f.Container)
	return err
}

// Kill SIGKILLs a container and starts it again on recovery.
type Kill struct{ Container string }

func (f Kill) Name() string { return "kill(" + f.Container + ")" }
func (f Kill) Inject(ctx context.Context) error {
	_, err := Run(ctx, "kill", "--signal", "KILL", f.Container)
	return err
}
func (f Kill) Recover(ctx context.Context) error {
	_, err := Run(ctx, "start", f.Container)
	return err
}

// Partition disconnects a container from a network and reconnects it on
// recovery. Reconnecting restores the container's original alias so peers can
// resolve it by name again.
type Partition struct {
	Container string
	Network   string
	Alias     string
}

func (f Partition) Name() string { return "partition(" + f.Container + " from " + f.Network + ")" }

func (f Partition) Inject(ctx context.Context) error {
	_, err := Run(ctx, "network", "disconnect", "-f", f.Network, f.Container)
	return err
}

func (f Partition) Recover(ctx context.Context) error {
	args := []string{"network", "connect"}
	if f.Alias != "" {
		args = append(args, "--alias", f.Alias)
	}
	args = append(args, f.Network, f.Container)
	_, err := Run(ctx, args...)
	return err
}

// WaitHealthy polls a check until it succeeds or the deadline passes. Used
// after Setup so a schedule never starts against a cluster that has not
// elected a leader yet: starting early is another way to record a history that
// proves nothing.
func WaitHealthy(ctx context.Context, timeout time.Duration, check func(context.Context) error) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := check(ctx); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("not healthy within %s: %w", timeout, last)
}
