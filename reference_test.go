package lincheck

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// This file is the checker's own regression test: two reference stores driven
// through the real adapter interface and the real driver, one correct and one
// deliberately broken. If the broken one ever stops being caught, the checker
// has regressed and every clean result it produces against a real system is
// worthless.

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
	prev map[string][]byte // the value before the most recent write, for the stale-read bug
	name string
	// staleReadRate is the probability a Get returns the previous value
	// instead of the current one. Zero for the correct store.
	staleReadRate float64
	// lostWriteRate is the probability a Put reports success without storing.
	lostWriteRate float64
	rng           *rand.Rand
	// latency models the round trip of a real store. It matters: with
	// microsecond-fast operations, a few concurrent clients produce a history
	// where almost everything overlaps everything, real-time order constrains
	// nothing, and proving non-linearizability becomes the worst case of an
	// exponential search. Real systems separate operations in time; the
	// reference stores model that rather than pretending it away.
	latency time.Duration
}

func (m *memStore) delay() {
	m.mu.Lock()
	d := m.latency/2 + time.Duration(m.rng.Int63n(int64(m.latency)))
	m.mu.Unlock()
	time.Sleep(d)
}

func newMemStore(name string, stale, lost float64, seed int64) *memStore {
	return &memStore{
		data: map[string][]byte{}, prev: map[string][]byte{},
		name: name, staleReadRate: stale, lostWriteRate: lost,
		rng: rand.New(rand.NewSource(seed)), latency: 2 * time.Millisecond,
	}
}

func (m *memStore) Name() string { return m.name }

func (m *memStore) Setup(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = map[string][]byte{}
	m.prev = map[string][]byte{}
	return nil
}

func (m *memStore) Teardown(context.Context) error { return nil }
func (m *memStore) Faults() []Fault                { return nil }

func (m *memStore) Clients(_ context.Context, n int) ([]Client, error) {
	out := make([]Client, n)
	for i := range out {
		out[i] = &memClient{s: m, node: fmt.Sprintf("mem-%d", i)}
	}
	return out, nil
}

type memClient struct {
	s    *memStore
	node string
}

func (c *memClient) Node() string { return c.node }
func (c *memClient) Close() error { return nil }

func (c *memClient) Get(_ context.Context, key string) ([]byte, bool, error) {
	c.s.delay()
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if c.s.staleReadRate > 0 && c.s.rng.Float64() < c.s.staleReadRate {
		if v, ok := c.s.prev[key]; ok {
			return append([]byte(nil), v...), true, nil
		}
	}
	v, ok := c.s.data[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (c *memClient) Put(_ context.Context, key string, value []byte) error {
	c.s.delay()
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if c.s.lostWriteRate > 0 && c.s.rng.Float64() < c.s.lostWriteRate {
		return nil // acknowledged, not stored
	}
	if old, ok := c.s.data[key]; ok {
		c.s.prev[key] = old
	}
	c.s.data[key] = append([]byte(nil), value...)
	return nil
}

func (c *memClient) Delete(_ context.Context, key string) error {
	c.s.delay()
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if old, ok := c.s.data[key]; ok {
		c.s.prev[key] = old
	}
	delete(c.s.data, key)
	return nil
}

func refConfig(seed int64) Config {
	cfg := DefaultConfig(seed)
	cfg.Clients = 3
	cfg.OpsPerClient = 80
	cfg.Keys = []string{"k0"}
	cfg.OpTimeout = time.Second
	// The reference stores model their own latency, so no extra think time.
	cfg.ThinkTime = 0
	cfg.ReadRatio = 0.5
	cfg.Require = Coverage{
		TotalOps: 240, CompletedOps: 240, CompletedGets: 50,
		ReadsOfRunValues: 20, MinOpsPerKey: 240,
	}
	return cfg
}

// TestCorrectReferenceStorePasses. A genuinely linearizable store must come
// back clean, or the checker produces false positives and no result it gives
// against a real system means anything.
func TestCorrectReferenceStorePasses(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		store := newMemStore("mem-correct", 0, 0, seed)
		run, err := RunSchedule(t.Context(), store, refConfig(seed), Options{Budget: 30 * time.Second})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !run.HarnessOK {
			t.Fatalf("seed %d: harness coverage floor not met: %v\n%s", seed, run.Shortfall, run.Coverage)
		}
		if v := run.Verdict(); v != Linearizable {
			t.Fatalf("seed %d: correct store reported %s\n%s", seed, v, Report(run.Results))
		}
		t.Logf("seed %d clean: %s", seed, run.Coverage)
	}
}

// TestBrokenReferenceStoreIsCaught is the detection regression test. A store
// that returns a stale value on 15% of reads is not linearizable, and the
// checker must say so and must print a minimized history proving it.
func TestBrokenReferenceStoreIsCaught(t *testing.T) {
	caught := 0
	total := 10
	for seed := int64(1); seed <= int64(total); seed++ {
		store := newMemStore("mem-stale-reads", 0.3, 0, seed)
		run, err := RunSchedule(t.Context(), store, refConfig(seed), Options{Budget: 30 * time.Second})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !run.HarnessOK {
			t.Fatalf("seed %d: harness coverage floor not met: %v", seed, run.Shortfall)
		}
		if run.Verdict() != Violation {
			t.Logf("seed %d: %s (not every seed has to trip it)", seed, run.Verdict())
			continue
		}
		caught++
		for _, v := range run.Violations() {
			if len(v.Violating) == 0 {
				t.Fatalf("seed %d: violation reported with no printable history", seed)
			}
			if seed == 1 {
				t.Logf("seed %d key %q minimized violating history (%d ops):\n%s",
					seed, v.Key, len(v.Violating), FormatHistory(v.Violating))
			}
		}
	}
	if caught < total*7/10 {
		t.Fatalf("stale-read store caught in only %d of %d seeds; "+
			"the checker's ability to detect is itself under test here", caught, total)
	}
}

// TestLostWriteStoreIsCaught covers the other failure mode: a store that
// acknowledges a write and does not store it. This is the shape of what an
// asynchronously replicated system loses on failover.
//
// It runs with a SINGLE client on purpose, and the reason is a real result
// rather than a convenience. With three concurrent writers the same store, at
// the same 30% loss rate, was flagged in 0 of 10 seeds: a lost write is almost
// always coverable by linearizing the lost Put earlier than the Put it
// overlapped, so concurrent writers hide lost writes from a linearizability
// checker. With one client the history is totally ordered in real time, there
// is no cover, and every lost write that a later read observes is a violation.
//
// The practical consequence for the real targets in this repo: a fault
// schedule aimed at lost writes wants low write concurrency and a read
// immediately after the write, not maximum load.
func TestLostWriteStoreIsCaught(t *testing.T) {
	caught := 0
	const seeds = 10
	for seed := int64(1); seed <= seeds; seed++ {
		store := newMemStore("mem-lost-writes", 0, 0.3, seed)
		cfg := refConfig(seed)
		cfg.Clients = 1
		cfg.OpsPerClient = 200
		cfg.Require = Coverage{
			TotalOps: 200, CompletedOps: 200, CompletedGets: 40,
			ReadsOfRunValues: 20, MinOpsPerKey: 200,
		}
		run, err := RunSchedule(t.Context(), store, cfg, Options{Budget: 30 * time.Second})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !run.HarnessOK {
			t.Fatalf("seed %d: coverage floor not met: %v", seed, run.Shortfall)
		}
		if run.Verdict() == Violation {
			caught++
			if seed == 1 {
				for _, v := range run.Violations() {
					t.Logf("seed 1 key %q minimized violating history (%d ops):\n%s",
						v.Key, len(v.Violating), FormatHistory(v.Violating))
				}
			}
		}
	}
	if caught < seeds*7/10 {
		t.Fatalf("lost-write store caught in only %d of %d seeds", caught, seeds)
	}
	t.Logf("lost-write store caught in %d of %d seeds", caught, seeds)
}

// ---------------------------------------------------------------------------
// Harness self-checks: the second lesson. A run that tested nothing must be
// reported as a harness failure, never as a pass.
// ---------------------------------------------------------------------------

// rejectingStore refuses every operation, the way a leaderless cluster does.
// The old harness bug was that clients burned their budget against exactly
// this and produced a near-empty history that still read as "pass".
type rejectingStore struct{ memStore }

func (r *rejectingStore) Name() string { return "always-rejecting" }
func (r *rejectingStore) Clients(_ context.Context, n int) ([]Client, error) {
	out := make([]Client, n)
	for i := range out {
		out[i] = rejectClient{node: fmt.Sprintf("dead-%d", i)}
	}
	return out, nil
}

type rejectClient struct{ node string }

func (c rejectClient) Node() string { return c.node }
func (c rejectClient) Close() error { return nil }
func (c rejectClient) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("no cluster leader: %w", ErrRejected)
}
func (c rejectClient) Put(context.Context, string, []byte) error {
	return fmt.Errorf("no cluster leader: %w", ErrRejected)
}
func (c rejectClient) Delete(context.Context, string) error {
	return fmt.Errorf("no cluster leader: %w", ErrRejected)
}

func TestEmptyHistoryIsAHarnessFailureNotAPass(t *testing.T) {
	store := &rejectingStore{}
	cfg := refConfig(1)
	cfg.RetryWindow = 200 * time.Millisecond
	cfg.RetryBackoff = 5 * time.Millisecond
	run, err := RunSchedule(t.Context(), store, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if run.HarnessOK {
		t.Fatal("a run that recorded nothing must not be reported as harness-OK")
	}
	if len(run.Results) != 0 {
		t.Fatal("no verdict may be computed for a run that failed its coverage floor")
	}
	if run.Coverage.GivenUpOps == 0 {
		t.Fatal("abandoned operations must be counted, otherwise they are invisible")
	}
	out := ReportRun(run)
	if !contains(out, "HARNESS FAILURE") {
		t.Fatalf("report must lead with the harness failure:\n%s", out)
	}
	t.Logf("%s", out)
}

// TestReadOnlyRunFailsCoverage: a run where every read returns "not found"
// proves nothing about a store, and the ReadsOfRunValues floor is what
// notices.
func TestReadOnlyRunFailsCoverage(t *testing.T) {
	cov := Coverage{TotalOps: 100, CompletedOps: 100, CompletedGets: 100, ReadsOfRunValues: 0, MinOpsPerKey: 50}
	req := Coverage{ReadsOfRunValues: 5}
	if len(req.Shortfall(cov)) == 0 {
		t.Fatal("a run whose reads never observed a value written by the run must fail its floor")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
