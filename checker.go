package lincheck

import (
	"fmt"
	"strings"
	"time"
	"unsafe"
)

// Verdict is the outcome of a check. There are three, not two, on purpose:
// Unknown is not a pass, and must never be collapsed into one.
type Verdict int

const (
	// Linearizable: a linearization was found and can be printed.
	Linearizable Verdict = iota
	// Violation: the search was exhaustive and no linearization exists. Only
	// this verdict may ever be reported as a bug in the system.
	Violation
	// Unknown: the search ran out of its time budget.
	Unknown
)

func (v Verdict) String() string {
	switch v {
	case Linearizable:
		return "LINEARIZABLE"
	case Violation:
		return "VIOLATION"
	default:
		return "UNKNOWN (search budget exhausted)"
	}
}

// Result is the outcome of checking one key's history.
type Result struct {
	Key     string
	Verdict Verdict
	OpCount int

	// Witness is a valid linearization when Verdict is Linearizable. It may be
	// shorter than the input: any in-doubt operation the search left out is
	// not part of this explanation.
	Witness []Op

	// Violating is the minimized sub-history that still fails to linearize,
	// set only when Verdict is Violation. A violation without one of these
	// must never be reported.
	Violating []Op

	// Segments is a concurrency statistic only; nothing depends on it for
	// correctness.
	Segments int

	Elapsed time.Duration
}

// register is the sequential model: one key's value, or its absence.
type register struct {
	exists bool
	value  string
}

// apply advances the model by one operation. ok is false when op could not
// have happened against this state, so it cannot be placed next.
func apply(s register, op Op) (next register, ok bool) {
	switch op.Type {
	case OpPut:
		return register{exists: true, value: string(op.Value)}, true
	case OpDelete:
		return register{}, true
	case OpGet:
		if op.ResultFound != s.exists {
			return s, false
		}
		if s.exists && op.ResultValue != nil && string(op.ResultValue) != s.value {
			return s, false
		}
		return s, true
	}
	return s, false
}

const (
	minimizeBudget = 3 * time.Second
	// DefaultBudget is the per-key wall-clock limit on the search.
	DefaultBudget = 60 * time.Second
)

// Options configures a check.
type Options struct {
	// Budget is the per-key search time limit. Exceeding it yields Unknown,
	// never Linearizable. Zero means DefaultBudget.
	Budget time.Duration
	// NoMinimize skips minimization of a violating history.
	NoMinimize bool
}

func (o Options) budget() time.Duration {
	if o.Budget <= 0 {
		return DefaultBudget
	}
	return o.Budget
}

// Check partitions a full history by key and runs CheckKey on each. That is
// sound for a single-key guarantee only; it is NOT sound for multi-key
// transactions, which this tool does not claim to check.
func Check(history []Op, opts Options) []Result {
	byKey := map[string][]Op{}
	var keys []string
	for _, op := range history {
		if _, seen := byKey[op.Key]; !seen {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	results := make([]Result, 0, len(keys))
	for _, k := range keys {
		results = append(results, CheckKey(k, byKey[k], opts))
	}
	return results
}

// CheckKey decides whether ops, every recorded operation on a single key,
// admits a linearization. See README.md for the search and its pruning.
//
// The search runs over the whole per-key history at once. Checking segments
// independently and threading one model state between them is unsound: a
// segment with two concurrent writes ends in two possible states, and a later
// read may be explained by either.
func CheckKey(key string, ops []Op, opts Options) Result {
	start := time.Now()
	deadline := start.Add(opts.budget())

	sorted := append([]Op(nil), ops...)
	sortByCall(sorted)
	res := Result{Key: key, OpCount: len(ops), Segments: len(segment(sorted))}

	order, v := search(sorted, deadline)
	res.Verdict = v
	switch v {
	case Linearizable:
		res.Witness = make([]Op, len(order))
		for i, idx := range order {
			res.Witness[i] = sorted[idx]
		}
	case Violation:
		res.Violating = sorted
		if !opts.NoMinimize {
			res.Violating = minimize(sorted, deadline)
		}
	}
	res.Elapsed = time.Since(start)
	return res
}

// segment cuts where real-time order forces complete separation: everything
// before the cut returned strictly before everything after it was called. A
// statistic only. An in-doubt operation blocks every later cut, since it has
// no Return and may still be pending arbitrarily far in the future.
func segment(sorted []Op) [][]Op {
	if len(sorted) == 0 {
		return nil
	}
	var segs [][]Op
	segStart := 0
	var maxReturn time.Time
	openDoubt := false
	for i := range sorted {
		if i > segStart && !openDoubt && !maxReturn.IsZero() && maxReturn.Before(sorted[i].Call) {
			segs = append(segs, sorted[segStart:i])
			segStart = i
			maxReturn = time.Time{}
		}
		if sorted[i].InDoubt {
			openDoubt = true
		} else if sorted[i].Return.After(maxReturn) {
			maxReturn = sorted[i].Return
		}
	}
	return append(segs, sorted[segStart:])
}

// bitset is a growable set of operation indices. Growable rather than a single
// uint64 so there is no 63-operation cap: a cap forces callers to truncate
// histories, which is the bug class this package exists to avoid.
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) has(i int) bool { return b[i/64]&(1<<uint(i%64)) != 0 }
func (b bitset) set(i int)      { b[i/64] |= 1 << uint(i%64) }
func (b bitset) clear(i int)    { b[i/64] &^= 1 << uint(i%64) }
func (b bitset) clone() bitset  { return append(bitset(nil), b...) }

// key renders the bitset as a map key without allocating on every memo probe.
// Safe only because the string never escapes the lookup and the bitset is
// cloned before it is stored.
func (b bitset) key() string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(&b[0])), len(b)*8)
}

type memoKey struct {
	mask  string
	state register
}

// search runs the linearization search over one key's full history, which must
// already be sorted by Call. It returns indices into ops in linearization
// order.
func search(ops []Op, deadline time.Time) ([]int, Verdict) {
	n := len(ops)
	if n == 0 {
		return nil, Linearizable
	}

	// A linearization must account for every non-in-doubt operation. In-doubt
	// ones are optional: the client never learned whether they took effect, so
	// leaving one out explains what happened just as faithfully as placing it.
	nRequired := 0
	for _, op := range ops {
		if !op.InDoubt {
			nRequired++
		}
	}

	// succ[j] lists the operations j is real-time-forced to precede. pending[i]
	// counts i's still-unplaced forced predecessors, making eligibility O(1)
	// instead of an O(n) scan per step; without it a long sequential history
	// costs O(n^3) and the checker stalls on histories it should handle
	// instantly.
	//
	// The comparison must stay strict. A wall clock's resolution is finite, so
	// two time.Now() calls can read back equal; under a non-strict check two
	// tied ops each look forced to precede the other, neither is ever eligible,
	// and a real linearization is missed. Treating ties as concurrent only
	// widens the search, so it cannot turn a violation into a false pass.
	succ := make([][]int, n)
	pending := make([]int, n)
	for j := 0; j < n; j++ {
		// An in-doubt op never returned, so nothing forces it to precede
		// anything: its interval runs to the end of time.
		if ops[j].InDoubt {
			continue
		}
		for i := 0; i < n; i++ {
			if i == j {
				continue
			}
			if ops[j].Return.Before(ops[i].Call) {
				succ[j] = append(succ[j], i)
				pending[i]++
			}
		}
	}

	unsolvable := map[memoKey]bool{}
	mask := newBitset(n)
	order := make([]int, 0, n)
	placedRequired := 0
	timedOut := false
	steps := 0

	var run func(state register) bool
	run = func(state register) bool {
		if placedRequired == nRequired {
			return true
		}
		steps++
		if steps&255 == 0 && time.Now().After(deadline) {
			timedOut = true
			return false
		}
		if unsolvable[memoKey{mask: mask.key(), state: state}] {
			return false
		}
		for i := 0; i < n; i++ {
			if mask.has(i) || pending[i] != 0 {
				continue
			}
			next, ok := apply(state, ops[i])
			if !ok {
				continue
			}
			mask.set(i)
			order = append(order, i)
			for _, k := range succ[i] {
				pending[k]--
			}
			if !ops[i].InDoubt {
				placedRequired++
			}
			if run(next) {
				return true
			}
			if !ops[i].InDoubt {
				placedRequired--
			}
			for _, k := range succ[i] {
				pending[k]++
			}
			order = order[:len(order)-1]
			mask.clear(i)
			if timedOut {
				return false
			}
		}
		unsolvable[memoKey{mask: mask.clone().key(), state: state}] = true
		return false
	}

	if run(register{}) {
		return append([]int(nil), order...), Linearizable
	}
	if timedOut {
		return nil, Unknown
	}
	return nil, Violation
}

// minimize shrinks a violating history by delta debugging: try to delete half
// the history, then quarters, down to single operations. Dropping operations
// only makes a history easier to linearize, so whatever still fails is
// strictly stronger evidence. One-at-a-time removal does not finish on a
// 300-operation history; chunks do, because the first few deletions do most of
// the work.
//
// The verdict is already decided here, so capping the budget at minimizeBudget
// only costs readability of the evidence, never correctness of the claim.
func minimize(ops []Op, deadline time.Time) []Op {
	if time.Until(deadline) > minimizeBudget {
		deadline = time.Now().Add(minimizeBudget)
	}

	cur := append([]Op(nil), ops...)
	for chunk := len(cur) / 2; chunk >= 1; chunk /= 2 {
		for progress := true; progress; {
			progress = false
			for i := 0; i < len(cur) && chunk < len(cur); i++ {
				if time.Now().After(deadline) {
					return cur
				}
				end := min(i+chunk, len(cur))
				cand := make([]Op, 0, len(cur)-(end-i))
				cand = append(cand, cur[:i]...)
				cand = append(cand, cur[end:]...)
				if !removable(cur[i:end], cand) {
					continue
				}
				if _, v := search(cand, deadline); v == Violation {
					cur = cand
					progress = true
					break
				}
			}
		}
	}
	return cur
}

// removable reports whether dropping the operations in drop, leaving cand,
// yields evidence still about the system rather than about the deletion. Two
// rules, both of which fabricate contradictions if dropped: a Delete may only
// go once every remaining read has finished, or a Get that legitimately found
// nothing now contradicts some earlier Put; and cand must stay well-formed, or
// the minimizer deletes the Put a Get observed and reports a value appearing
// from nowhere.
func removable(drop, cand []Op) bool {
	for _, op := range drop {
		if op.Type == OpDelete && !afterEveryRead(op, cand) {
			return false
		}
	}
	return wellFormed(cand)
}

// afterEveryRead reports whether op was called only after every Get in ops had
// already returned.
func afterEveryRead(op Op, ops []Op) bool {
	for _, o := range ops {
		if o.Type != OpGet {
			continue
		}
		if o.InDoubt || !o.Return.Before(op.Call) {
			return false
		}
	}
	return true
}

// wellFormed reports whether every value a Get observed is written by some Put
// still present in the history.
func wellFormed(ops []Op) bool {
	written := map[string]bool{}
	for _, op := range ops {
		if op.Type == OpPut {
			written[string(op.Value)] = true
		}
	}
	for _, op := range ops {
		if op.Type == OpGet && op.ResultFound && !written[string(op.ResultValue)] {
			return false
		}
	}
	return true
}

// Report renders results for humans. A violation always prints its full
// minimized history: an unprintable violation is not a claim this tool makes.
func Report(results []Result) string {
	var b strings.Builder
	for _, r := range results {
		fmt.Fprintf(&b, "key %q: %s (%d ops, %d segments, %s)\n",
			r.Key, r.Verdict, r.OpCount, r.Segments, r.Elapsed.Round(time.Millisecond))
		if r.Verdict == Violation {
			fmt.Fprintf(&b, "  minimized violating history (%d ops, no linearization exists):\n", len(r.Violating))
			b.WriteString(indent(FormatHistory(r.Violating), "  "))
		}
	}
	return b.String()
}

func indent(s, pad string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n") + "\n"
}
