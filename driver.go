package lincheck

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Config controls one schedule: one seeded run against one Target.
type Config struct {
	Seed         int64
	Clients      int
	OpsPerClient int
	Keys         []string

	// ThinkTime is an upper bound on the random pause a client takes between
	// operations, and it is not padding. Too small and the schedule finishes
	// before the first fault fires (60 ops against a local etcd cluster take
	// under 100ms). It also spaces the history out: operations that all
	// overlap carry almost no real-time ordering information, which weakens
	// the check and makes proving a violation exponentially expensive.
	ThinkTime time.Duration

	// ReadRatio is the fraction of operations that are reads. A history
	// dominated by writes hides lost writes: the next write overwrites the key
	// within milliseconds, so no read ever observes the stale value. Reads are
	// what turn a lost write into evidence.
	ReadRatio float64

	// OpTimeout bounds a single client operation. An operation that exceeds it
	// is recorded as in-doubt, not dropped.
	OpTimeout time.Duration

	// RetryWindow is how long a client keeps retrying an operation that every
	// node definitively REJECTED before giving up. During a leaderless window
	// every node rejects instantly, so without retrying a client burns its
	// whole budget in milliseconds and records almost nothing while the run
	// still reads as "pass"; this and the Coverage floor are the two halves of
	// the fix.
	RetryWindow  time.Duration
	RetryBackoff time.Duration

	// Fault schedule. FaultDelay is the quiet period before the first fault;
	// FaultHold is how long a fault stays injected; FaultGap is the healthy
	// period between faults. Faults are chosen from Target.Faults with the
	// seeded RNG.
	FaultDelay time.Duration
	FaultHold  time.Duration
	FaultGap   time.Duration

	// Require sets the coverage floor. A run that does not meet it is reported
	// as a HARNESS FAILURE, never as a pass.
	Require Coverage
}

// DefaultConfig is a reasonable starting point for a real 3-node cluster.
func DefaultConfig(seed int64) Config {
	return Config{
		Seed:         seed,
		Clients:      6,
		OpsPerClient: 40,
		Keys:         []string{"k0", "k1", "k2"},
		ReadRatio:    0.5,
		ThinkTime:    250 * time.Millisecond,
		OpTimeout:    2 * time.Second,
		RetryWindow:  8 * time.Second,
		RetryBackoff: 20 * time.Millisecond,
		FaultDelay:   1 * time.Second,
		FaultHold:    4 * time.Second,
		FaultGap:     3 * time.Second,
		Require: Coverage{
			TotalOps:         40,
			CompletedOps:     20,
			CompletedGets:    8,
			ReadsOfRunValues: 4,
			MinOpsPerKey:     4,
		},
	}
}

// Coverage is what the run actually managed to exercise: both the measured
// result and, via Config.Require, the floor a run must clear. A history of
// three operations trivially linearizes, and without these numbers such a run
// is indistinguishable from a real one.
type Coverage struct {
	TotalOps     int
	CompletedOps int
	InDoubtOps   int
	GivenUpOps   int

	CompletedGets int
	CompletedPuts int

	// ReadsOfRunValues counts Gets that returned a value written by this run,
	// the strongest single signal that the harness was actually talking to the
	// system: reads that only ever return "not found" prove nothing.
	ReadsOfRunValues int

	MinOpsPerKey int
	KeysTouched  int

	FaultsInjected int
	Duration       time.Duration
}

// Shortfall returns the reasons cov fails to meet the floor req, or nil.
func (req Coverage) Shortfall(cov Coverage) []string {
	var bad []string
	chk := func(name string, got, want int) {
		if want > 0 && got < want {
			bad = append(bad, fmt.Sprintf("%s = %d, required >= %d", name, got, want))
		}
	}
	chk("TotalOps", cov.TotalOps, req.TotalOps)
	chk("CompletedOps", cov.CompletedOps, req.CompletedOps)
	chk("CompletedGets", cov.CompletedGets, req.CompletedGets)
	chk("CompletedPuts", cov.CompletedPuts, req.CompletedPuts)
	chk("ReadsOfRunValues", cov.ReadsOfRunValues, req.ReadsOfRunValues)
	chk("MinOpsPerKey", cov.MinOpsPerKey, req.MinOpsPerKey)
	chk("FaultsInjected", cov.FaultsInjected, req.FaultsInjected)
	return bad
}

func (c Coverage) String() string {
	return fmt.Sprintf(
		"ops=%d completed=%d (get=%d put=%d) in-doubt=%d given-up=%d "+
			"reads-of-run-values=%d keys=%d min-ops-per-key=%d faults=%d duration=%s",
		c.TotalOps, c.CompletedOps, c.CompletedGets, c.CompletedPuts,
		c.InDoubtOps, c.GivenUpOps, c.ReadsOfRunValues, c.KeysTouched,
		c.MinOpsPerKey, c.FaultsInjected, c.Duration.Round(time.Millisecond))
}

// Run is one schedule's outcome.
type Run struct {
	Target   string
	Seed     int64
	History  []Op
	Coverage Coverage
	// HarnessOK is false when Coverage did not meet Config.Require. When it is
	// false the Results below are meaningless and must not be reported as a
	// pass.
	HarnessOK bool
	Shortfall []string
	Results   []Result
	FaultLog  []string
}

// Verdict summarizes a run: the worst thing that happened.
func (r Run) Verdict() Verdict {
	worst := Linearizable
	for _, res := range r.Results {
		if res.Verdict == Violation {
			return Violation
		}
		if res.Verdict == Unknown {
			worst = Unknown
		}
	}
	return worst
}

// Violations returns the results that failed to linearize.
func (r Run) Violations() []Result {
	var out []Result
	for _, res := range r.Results {
		if res.Verdict == Violation {
			out = append(out, res)
		}
	}
	return out
}

// RunSchedule executes one seeded schedule against a Target and checks it.
func RunSchedule(ctx context.Context, target Target, cfg Config, opts Options) (Run, error) {
	run := Run{Target: target.Name(), Seed: cfg.Seed}

	if err := target.Setup(ctx); err != nil {
		return run, fmt.Errorf("setup: %w", err)
	}

	clients, err := target.Clients(ctx, cfg.Clients)
	if err != nil {
		return run, fmt.Errorf("clients: %w", err)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	rec := NewRecorder()
	started := time.Now()
	done := make(chan struct{})

	// Fault scheduler.
	var faultMu sync.Mutex
	var faultLog []string
	faultsInjected := 0
	logFault := func(at time.Duration, what string, f Fault, err error) {
		line := fmt.Sprintf("%8s %s %s", at.Round(time.Millisecond), what, f.Name())
		if err != nil {
			line += fmt.Sprintf(" FAILED: %v", err)
		}
		faultMu.Lock()
		if err == nil && what == "inject" {
			faultsInjected++
		}
		faultLog = append(faultLog, line)
		faultMu.Unlock()
	}
	faults := target.Faults()
	var faultWG sync.WaitGroup
	if len(faults) > 0 {
		faultWG.Add(1)
		go func() {
			defer faultWG.Done()
			rng := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
			if sleepOrDone(done, cfg.FaultDelay) {
				return
			}
			for {
				f := faults[rng.Intn(len(faults))]
				at := time.Since(started)
				logFault(at, "inject", f, f.Inject(ctx))

				stop := sleepOrDone(done, cfg.FaultHold)
				// Recover unconditionally, including on abort.
				rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
				rerr := f.Recover(rctx)
				cancel()
				logFault(time.Since(started), "recover", f, rerr)

				if stop || sleepOrDone(done, cfg.FaultGap) {
					return
				}
			}
		}()
	}

	// Clients.
	var clientWG sync.WaitGroup
	clientWG.Add(len(clients))
	for i, c := range clients {
		go func(id int, cl Client) {
			defer clientWG.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(id)*7919))
			for n := 0; n < cfg.OpsPerClient; n++ {
				if cfg.ThinkTime > 0 {
					time.Sleep(time.Duration(rng.Int63n(int64(cfg.ThinkTime))))
				}
				key := cfg.Keys[rng.Intn(len(cfg.Keys))]
				doOne(ctx, rec, cl, id, key, n, cfg, rng)
			}
		}(i, c)
	}
	clientWG.Wait()
	close(done)
	faultWG.Wait()

	run.History = rec.Ops()
	run.Coverage = measure(run.History, rec.GivenUp(), faultsInjected, time.Since(started))
	faultMu.Lock()
	run.FaultLog = append([]string(nil), faultLog...)
	faultMu.Unlock()

	run.Shortfall = cfg.Require.Shortfall(run.Coverage)
	run.HarnessOK = len(run.Shortfall) == 0
	if run.HarnessOK {
		run.Results = Check(run.History, opts)
	}
	return run, nil
}

// doOne performs one logical client operation, retrying only while the system
// DEFINITIVELY rejects it. An indeterminate outcome ends the attempt: it is a
// real event that must be recorded, not retried away.
func doOne(ctx context.Context, rec *Recorder, cl Client, id int, key string, n int, cfg Config, rng *rand.Rand) {
	deadline := time.Now().Add(cfg.RetryWindow)
	ratio := cfg.ReadRatio
	if ratio <= 0 {
		ratio = 0.5
	}
	write := rng.Float64() >= ratio
	del := write && rng.Intn(12) == 0
	value := fmt.Sprintf("c%d-n%d-s%d", id, n, cfg.Seed)

	for {
		octx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
		call := time.Now()
		var (
			err   error
			val   []byte
			found bool
			typ   OpType
		)
		switch {
		case del:
			typ = OpDelete
			err = cl.Delete(octx, key)
		case write:
			typ = OpPut
			err = cl.Put(octx, key, []byte(value))
		default:
			typ = OpGet
			val, found, err = cl.Get(octx, key)
		}
		ret := time.Now()
		cancel()

		op := Op{Client: id, Node: cl.Node(), Key: key, Type: typ, Call: call, Return: ret}
		if typ == OpPut {
			op.Value = []byte(value)
		}
		if err == nil {
			op.ResultFound = found
			op.ResultValue = val
			rec.Record(op)
			return
		}
		if Rejected(err) {
			// Definitively did not happen. Nothing to record. Retry.
			if time.Now().After(deadline) {
				rec.GaveUp()
				return
			}
			time.Sleep(cfg.RetryBackoff)
			continue
		}
		// Indeterminate.
		op.InDoubt = true
		op.Err = truncErr(err)
		rec.Record(op)
		return
	}
}

func truncErr(err error) string {
	s := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

func measure(history []Op, givenUp, faults int, dur time.Duration) Coverage {
	cov := Coverage{TotalOps: len(history), GivenUpOps: givenUp, FaultsInjected: faults, Duration: dur}
	perKey := map[string]int{}
	written := map[string]bool{}
	for _, op := range history {
		perKey[op.Key]++
		if op.Type == OpPut {
			written[string(op.Value)] = true
		}
	}
	for _, op := range history {
		if op.InDoubt {
			cov.InDoubtOps++
			continue
		}
		cov.CompletedOps++
		switch op.Type {
		case OpGet:
			cov.CompletedGets++
			if op.ResultFound && written[string(op.ResultValue)] {
				cov.ReadsOfRunValues++
			}
		case OpPut:
			cov.CompletedPuts++
		}
	}
	cov.KeysTouched = len(perKey)
	for _, n := range perKey {
		if cov.MinOpsPerKey == 0 || n < cov.MinOpsPerKey {
			cov.MinOpsPerKey = n
		}
	}
	return cov
}

func sleepOrDone(done <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// ReportRun renders a full run, coverage first: a run that missed its floor
// has no verdict worth reading.
func ReportRun(r Run) string {
	var b strings.Builder
	fmt.Fprintf(&b, "target=%s seed=%d\n", r.Target, r.Seed)
	fmt.Fprintf(&b, "coverage: %s\n", r.Coverage)
	if !r.HarnessOK {
		fmt.Fprintf(&b, "HARNESS FAILURE: coverage floor not met, no verdict is claimed for this schedule\n")
		for _, s := range r.Shortfall {
			fmt.Fprintf(&b, "  %s\n", s)
		}
		return b.String()
	}
	if len(r.FaultLog) > 0 {
		b.WriteString("fault schedule:\n")
		for _, l := range r.FaultLog {
			fmt.Fprintf(&b, "  %s\n", l)
		}
	}
	fmt.Fprintf(&b, "verdict: %s\n", r.Verdict())
	b.WriteString(Report(r.Results))
	return b.String()
}
