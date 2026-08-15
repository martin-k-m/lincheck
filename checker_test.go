package lincheck

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func put(client int, key, val string, call, ret int) Op {
	return Op{Client: client, Key: key, Type: OpPut, Value: []byte(val), Call: at(call), Return: at(ret)}
}

func get(client int, key, val string, found bool, call, ret int) Op {
	op := Op{Client: client, Key: key, Type: OpGet, ResultFound: found, Call: at(call), Return: at(ret)}
	if found {
		op.ResultValue = []byte(val)
	}
	return op
}

func del(client int, key string, call, ret int) Op {
	return Op{Client: client, Key: key, Type: OpDelete, Call: at(call), Return: at(ret)}
}

func checkOne(t *testing.T, ops []Op) Result {
	t.Helper()
	res := CheckKey("k", ops, Options{Budget: 20 * time.Second})
	return res
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	res := checkOne(t, []Op{
		put(0, "k", "a", 0, 10),
		get(0, "k", "a", true, 20, 30),
		put(0, "k", "b", 40, 50),
		get(0, "k", "b", true, 60, 70),
		del(0, "k", 80, 90),
		get(0, "k", "", false, 100, 110),
	})
	if res.Verdict != Linearizable {
		t.Fatalf("want LINEARIZABLE, got %s\n%s", res.Verdict, FormatHistory(res.Violating))
	}
	if len(res.Witness) != 6 {
		t.Fatalf("witness should account for all 6 ops, got %d", len(res.Witness))
	}
}

func TestConcurrentWritesEitherOrder(t *testing.T) {
	// Two overlapping writes, then a read of one of them. Both orders are
	// legal, so this must linearize whichever value the read saw.
	for _, seen := range []string{"a", "b"} {
		res := checkOne(t, []Op{
			put(0, "k", "a", 0, 100),
			put(1, "k", "b", 10, 110),
			get(2, "k", seen, true, 120, 130),
		})
		if res.Verdict != Linearizable {
			t.Fatalf("read of %q should linearize, got %s", seen, res.Verdict)
		}
	}
}

// TestStaleReadIsCaught is the core detection test: a read that returns an old
// value strictly after the write of a new value completed cannot be explained.
func TestStaleReadIsCaught(t *testing.T) {
	res := checkOne(t, []Op{
		put(0, "k", "a", 0, 10),
		put(0, "k", "b", 20, 30),
		get(1, "k", "a", true, 40, 50), // stale: "b" is committed and "a" is gone
	})
	if res.Verdict != Violation {
		t.Fatalf("want VIOLATION, got %s", res.Verdict)
	}
	if len(res.Violating) == 0 {
		t.Fatal("violation reported with no printable history")
	}
	out := FormatHistory(res.Violating)
	if !strings.Contains(out, "Get") {
		t.Fatalf("minimized history lost the read:\n%s", out)
	}
	t.Logf("minimized violating history:\n%s", out)
}

func TestLostWriteIsCaught(t *testing.T) {
	// A completed write is acknowledged and then a later read finds nothing.
	// No linearization can place a completed Put before a Get that says the
	// key is absent, with no Delete anywhere in the history.
	res := checkOne(t, []Op{
		put(0, "k", "a", 0, 10),
		get(1, "k", "", false, 20, 30),
	})
	if res.Verdict != Violation {
		t.Fatalf("want VIOLATION for a lost acknowledged write, got %s", res.Verdict)
	}
	t.Logf("minimized violating history:\n%s", FormatHistory(res.Violating))
}

func TestMinimizationShrinksHistory(t *testing.T) {
	ops := []Op{
		put(0, "k", "x1", 0, 10),
		get(0, "k", "x1", true, 11, 12),
		put(0, "k", "x2", 13, 14),
		get(0, "k", "x2", true, 15, 16),
		put(0, "k", "a", 20, 30),
		put(0, "k", "b", 40, 50),
		get(1, "k", "a", true, 60, 70), // the actual contradiction
		put(0, "k", "c", 80, 90),
		get(0, "k", "c", true, 91, 92),
	}
	res := checkOne(t, ops)
	if res.Verdict != Violation {
		t.Fatalf("want VIOLATION, got %s", res.Verdict)
	}
	if len(res.Violating) >= len(ops) {
		t.Fatalf("minimization did not shrink: %d of %d ops", len(res.Violating), len(ops))
	}
	t.Logf("minimized from %d to %d ops:\n%s", len(ops), len(res.Violating), FormatHistory(res.Violating))
}

// ---------------------------------------------------------------------------
// In-doubt soundness. Both directions were real bugs.
// ---------------------------------------------------------------------------

// TestInDoubtWriteDoesNotCauseFalseViolation: a write that timed out and then
// committed anyway. Modelling it as "did not happen" makes the later read
// unexplainable and reports a violation that is not there.
func TestInDoubtWriteDoesNotCauseFalseViolation(t *testing.T) {
	inDoubt := Op{Client: 1, Key: "k", Type: OpPut, Value: []byte("b"), Call: at(20), InDoubt: true}
	ops := []Op{
		put(0, "k", "a", 0, 10),
		inDoubt,
		get(2, "k", "b", true, 60, 70),
	}
	res := checkOne(t, ops)
	if res.Verdict != Linearizable {
		t.Fatalf("in-doubt write must be placeable; got %s\n%s", res.Verdict, FormatHistory(res.Violating))
	}

	// Same history with the in-doubt flag stripped and the op dropped, which
	// is what the old harness did: now it is a false violation. This asserts
	// the bug is real and that InDoubt is what fixes it.
	dropped := []Op{ops[0], ops[2]}
	if CheckKey("k", dropped, Options{}).Verdict != Violation {
		t.Fatal("expected dropping the in-doubt write to produce a FALSE violation; " +
			"if it does not, this regression test no longer proves anything")
	}
}

// TestInDoubtWriteCanMaskNothing: the other direction. An in-doubt write is
// optional, so it must not be usable to explain away a genuine contradiction
// that does not involve it.
func TestInDoubtWriteDoesNotHideRealViolation(t *testing.T) {
	ops := []Op{
		put(0, "k", "a", 0, 10),
		put(0, "k", "b", 20, 30),
		{Client: 1, Key: "k", Type: OpPut, Value: []byte("z"), Call: at(200), InDoubt: true},
		get(2, "k", "a", true, 40, 50), // stale read, unrelated to the in-doubt op
	}
	res := checkOne(t, ops)
	if res.Verdict != Violation {
		t.Fatalf("an unrelated in-doubt write must not launder a real violation; got %s", res.Verdict)
	}
	t.Logf("minimized violating history:\n%s", FormatHistory(res.Violating))
}

// TestInDoubtWriteMayBeOmitted: the in-doubt op never took effect, and a later
// read proves it. That is also legal.
func TestInDoubtWriteMayBeOmitted(t *testing.T) {
	res := checkOne(t, []Op{
		put(0, "k", "a", 0, 10),
		{Client: 1, Key: "k", Type: OpPut, Value: []byte("b"), Call: at(20), InDoubt: true},
		get(2, "k", "a", true, 60, 70),
	})
	if res.Verdict != Linearizable {
		t.Fatalf("omitting an in-doubt write must be legal; got %s", res.Verdict)
	}
}

// ---------------------------------------------------------------------------
// Structural properties: no cap, no truncation, ties are concurrent.
// ---------------------------------------------------------------------------

// TestNoOperationCap runs well past the 63-op limit the original bitmask
// implementation had. That limit forced callers to truncate histories, which
// is exactly the silent-under-testing bug this package is built to avoid.
func TestNoOperationCap(t *testing.T) {
	var ops []Op
	for i := 0; i < 400; i++ {
		v := string(rune('a' + i%26))
		ops = append(ops, put(0, "k", v, i*10, i*10+2))
		ops = append(ops, get(0, "k", v, true, i*10+4, i*10+6))
	}
	res := CheckKey("k", ops, Options{Budget: 20 * time.Second})
	if res.Verdict != Linearizable {
		t.Fatalf("800-op sequential history should linearize, got %s", res.Verdict)
	}
	if res.OpCount != 800 {
		t.Fatalf("OpCount = %d, want 800: the checker must never silently drop ops", res.OpCount)
	}
	if res.Segments < 100 {
		t.Fatalf("expected heavy segmentation of a sequential history, got %d segments", res.Segments)
	}
}

// TestTiedTimestampsTreatedAsConcurrent. Two ops whose recorded instants are
// equal must not each be considered forced to precede the other, which would
// make a linearizable history unlinearizable.
func TestTiedTimestampsTreatedAsConcurrent(t *testing.T) {
	a := Op{Client: 0, Key: "k", Type: OpPut, Value: []byte("a"), Call: at(0), Return: at(10)}
	b := Op{Client: 1, Key: "k", Type: OpPut, Value: []byte("b"), Call: at(10), Return: at(10)}
	g := Op{Client: 2, Key: "k", Type: OpGet, ResultFound: true, ResultValue: []byte("a"), Call: at(10), Return: at(10)}
	if v := CheckKey("k", []Op{a, b, g}, Options{}).Verdict; v != Linearizable {
		t.Fatalf("tied instants must be concurrent, got %s", v)
	}
}

// TestBudgetExhaustionIsUnknownNotPass. A search that runs out of time must
// never be recorded as clean.
func TestBudgetExhaustionIsUnknownNotPass(t *testing.T) {
	// A wide band of mutually concurrent writes plus one read that can only be
	// satisfied late, which forces deep search. Budget is set to effectively
	// zero so the deadline trips immediately.
	var ops []Op
	for i := 0; i < 40; i++ {
		ops = append(ops, put(i, "k", string(rune('a'+i%26))+string(rune('0'+i/26)), 0, 1000))
	}
	ops = append(ops, get(99, "k", "nope", true, 0, 1000))
	res := CheckKey("k", ops, Options{Budget: time.Nanosecond})
	if res.Verdict != Unknown {
		t.Fatalf("an exhausted budget must be UNKNOWN, got %s", res.Verdict)
	}
	if strings.Contains(res.Verdict.String(), "LINEARIZABLE") {
		t.Fatal("UNKNOWN must never render as a pass")
	}
}

func TestSegmentationMatchesUnsegmented(t *testing.T) {
	// A history that segments, with a violation in the second segment. The
	// verdict must be identical to checking it as one block.
	ops := []Op{
		put(0, "k", "a", 0, 10),
		get(0, "k", "a", true, 20, 30),
		put(0, "k", "b", 100, 110),
		put(0, "k", "c", 120, 130),
		get(1, "k", "b", true, 140, 150),
	}
	res := CheckKey("k", ops, Options{})
	if res.Verdict != Violation {
		t.Fatalf("want VIOLATION, got %s", res.Verdict)
	}
	if res.Segments < 2 {
		t.Fatalf("expected this history to segment, got %d", res.Segments)
	}
}

func TestReportPrintsFullMinimizedHistory(t *testing.T) {
	res := CheckKey("k", []Op{
		put(0, "k", "a", 0, 10),
		put(0, "k", "b", 20, 30),
		get(1, "k", "a", true, 40, 50),
	}, Options{})
	out := Report([]Result{res})
	if !strings.Contains(out, "VIOLATION") || !strings.Contains(out, "minimized violating history") {
		t.Fatalf("report must print the evidence:\n%s", out)
	}
	// Every op in Violating must appear in the printed report.
	for _, op := range res.Violating {
		if !strings.Contains(out, op.String()) {
			t.Fatalf("op %q missing from report:\n%s", op, out)
		}
	}
}
