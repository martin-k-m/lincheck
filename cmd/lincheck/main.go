// Command lincheck runs seeded fault-injection schedules against a real system
// and checks the recorded history for single-key linearizability.
//
//	lincheck -target etcd -schedules 10 -seed 1
//
// Exit status is 0 for a clean run, 1 for a violation, and 2 for a run whose
// coverage floor was not met or whose search budget was exhausted. Those last
// two are NOT passes and do not share an exit code with one.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/martin-k-m/lincheck"
	"github.com/martin-k-m/lincheck/dockerfault"
	"github.com/martin-k-m/lincheck/targets/consul"
	"github.com/martin-k-m/lincheck/targets/etcd"
	"github.com/martin-k-m/lincheck/targets/redis"
)

func main() {
	// os.Exit skips deferred functions, and the deferred function here is the
	// one that removes the containers. Running the body in a helper and
	// exiting only after it returns is what keeps a run from leaking a live
	// cluster and its published ports.
	os.Exit(runMain())
}

// runMain wraps run so that fatal's panic unwinds through run's deferred
// teardown and still produces a clean exit status rather than a stack trace.
func runMain() (code int) {
	defer func() {
		if r := recover(); r != nil {
			if r == errAbort {
				code = 2
				return
			}
			panic(r)
		}
	}()
	return run()
}

func run() int {
	var (
		targetName = flag.String("target", "etcd", "target: etcd, consul, redis")
		schedules  = flag.Int("schedules", 5, "number of seeded schedules to run")
		seed0      = flag.Int64("seed", 1, "first seed; schedule i uses seed+i")
		clients    = flag.Int("clients", 6, "concurrent clients")
		ops        = flag.Int("ops", 40, "operations per client")
		keys       = flag.String("keys", "k0,k1,k2", "comma-separated key pool")
		budget     = flag.Duration("budget", 60*time.Second, "per-key search budget")
		faultHold  = flag.Duration("fault-hold", 4*time.Second, "how long each fault stays injected")
		faultGap   = flag.Duration("fault-gap", 3*time.Second, "healthy period between faults")
		think      = flag.Duration("think", 250*time.Millisecond, "upper bound on client pause between operations")
		readRatio  = flag.Float64("read-ratio", 0.5, "fraction of operations that are reads")
		keep       = flag.Bool("keep", false, "leave containers running after the run")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := dockerfault.Available(ctx); err != nil {
		fatal("docker is required: %v", err)
	}
	dv, _ := dockerfault.ServerVersion(ctx)

	var target lincheck.Target
	switch *targetName {
	case "etcd":
		target = etcd.New()
	case "consul":
		target = consul.New()
	case "redis":
		target = redis.New()
	default:
		fatal("unknown target %q", *targetName)
	}

	fmt.Printf("lincheck\n")
	fmt.Printf("target:          %s\n", target.Name())
	fmt.Printf("docker daemon:   %s\n", dv)
	if d, ok := target.(lincheck.Documented); ok {
		fmt.Printf("documented model: %s\n", wrap(d.DocumentedModel(), 78, "                  "))
	}
	fmt.Printf("schedules:       %d (seeds %d..%d)\n", *schedules, *seed0, *seed0+int64(*schedules)-1)
	fmt.Printf("clients:         %d, ops/client: %d, keys: %s\n", *clients, *ops, *keys)
	fmt.Printf("fault set:       %s\n", strings.Join(faultNames(target), ", "))
	fmt.Printf("fault schedule:  first at 1s, hold %s, gap %s, uniformly at random from the fault set with the schedule seed\n", *faultHold, *faultGap)
	fmt.Println()

	defer func() {
		if !*keep {
			tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			_ = target.Teardown(tctx)
		}
	}()

	worst := 0
	totalOps, totalCompleted, totalInDoubt, totalFaults := 0, 0, 0, 0
	for i := 0; i < *schedules; i++ {
		seed := *seed0 + int64(i)
		cfg := lincheck.DefaultConfig(seed)
		cfg.Clients = *clients
		cfg.OpsPerClient = *ops
		cfg.Keys = strings.Split(*keys, ",")
		cfg.ThinkTime = *think
		cfg.ReadRatio = *readRatio
		cfg.FaultHold = *faultHold
		cfg.FaultGap = *faultGap
		cfg.Require = lincheck.Coverage{
			TotalOps:         *clients * *ops / 4,
			CompletedOps:     *clients * *ops / 8,
			CompletedGets:    10,
			CompletedPuts:    10,
			ReadsOfRunValues: 5,
			MinOpsPerKey:     5,
			FaultsInjected:   1,
		}

		run, err := lincheck.RunSchedule(ctx, target, cfg, lincheck.Options{Budget: *budget})
		if err != nil {
			fatal("schedule seed %d: %v", seed, err)
		}
		fmt.Printf("---- schedule %d/%d ----\n%s\n", i+1, *schedules, lincheck.ReportRun(run))

		totalOps += run.Coverage.TotalOps
		totalCompleted += run.Coverage.CompletedOps
		totalInDoubt += run.Coverage.InDoubtOps
		totalFaults += run.Coverage.FaultsInjected

		switch {
		case !run.HarnessOK:
			worst = max(worst, 2)
		case run.Verdict() == lincheck.Violation:
			worst = max(worst, 1)
		case run.Verdict() == lincheck.Unknown:
			worst = max(worst, 2)
		}
		if ctx.Err() != nil {
			break
		}
	}

	fmt.Printf("==== totals over %d schedules ====\n", *schedules)
	fmt.Printf("operations recorded: %d, completed: %d, in doubt: %d, faults injected: %d\n",
		totalOps, totalCompleted, totalInDoubt, totalFaults)
	switch worst {
	case 0:
		fmt.Println("result: no linearizability violation found")
	case 1:
		fmt.Println("result: VIOLATION, see the minimized histories above")
	default:
		fmt.Println("result: INCONCLUSIVE, at least one schedule failed its coverage floor or its search budget")
	}
	return worst
}

func faultNames(t lincheck.Target) []string {
	var out []string
	for _, f := range t.Faults() {
		out = append(out, f.Name())
	}
	if len(out) == 0 {
		return []string{"(none)"}
	}
	return out
}

func wrap(s string, width int, pad string) string {
	var lines []string
	cur := ""
	for _, w := range strings.Fields(s) {
		if len(cur)+len(w)+1 > width {
			lines = append(lines, cur)
			cur = w
			continue
		}
		if cur == "" {
			cur = w
		} else {
			cur += " " + w
		}
	}
	lines = append(lines, cur)
	return strings.Join(lines, "\n"+pad)
}

// errAbort is the panic value fatal uses. Panicking rather than calling
// os.Exit is what lets run's deferred teardown remove the containers before
// the process ends.
var errAbort = fmt.Errorf("lincheck aborted")

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "lincheck: "+format+"\n", args...)
	panic(errAbort)
}
