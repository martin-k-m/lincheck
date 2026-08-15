package lincheck

import (
	"fmt"
	"strings"
	"time"
	"unsafe"
)

// Verdict is the outcome of a check. There are three, not two, on purpose.
type Verdict int

const (
	// Linearizable: a linearization was found and can be printed.
	Linearizable Verdict = iota
	// Violation: the search was exhaustive and no linearization exists.
	// Only this verdict may ever be reported as a bug in the system.
	Violation
	// Unknown: the search ran out of its time budget. This is NOT a pass. It
	// is reported separately so that a run which checked nothing can never be
	// mistaken for a run which checked something and found it clean.
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
	// shorter than the input: any in-doubt operation the search chose to leave
	// out is not part of this explanation.
	Witness []Op

	// Violating is the minimized sub-history that still fails to linearize,
	// set only when Verdict is Violation. This is what gets printed. A
	// violation without one of these must never be reported.
	Violating []Op

	// Segments is how many independent real-time segments the key's history
	// falls into: a description of how concurrent the history was. Nothing
	// depends on it for correctness.
	Segments int

	// Elapsed is how long the search took.
	Elapsed time.Duration
}

// register is the sequential model a linearization is checked against: one
// key's value, or its absence.
type register struct {
	exists bool
	value  string
}

// apply advances the model by one operation. ok is false when op could not
// have happened against this state, meaning it cannot be placed next.
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

// minimizeBudget caps how long shrinking a violating history may take. The
// verdict is already decided by then, so a partially minimized history is an
// acceptable outcome; an unminimized one is still correct, just harder to read.
const minimizeBudget = 3 * time.Second

// DefaultBudget is the per-key wall-clock limit on the search.
const DefaultBudget = 60 * time.Second

// Options configures a check.
type Options struct {
	// Budget is the per-key search time limit. Exceeding it yields Unknown,
	// never Linearizable. Zero means DefaultBudget.
	Budget time.Duration
	// NoMinimize skips minimization of a violating history. Minimization is a
	// sequence of re-checks and is on by default, because an unminimized
	// violation is close to unusable as evidence.
	NoMinimize bool
}

func (o Options) budget() time.Duration {
	if o.Budget <= 0 {
		return DefaultBudget
	}
	return o.Budget
}

// Check partitions a full history by key and runs CheckKey on each,
// independently. That is sound for a single-key guarantee: one key's history
// can never be forced inconsistent by another key's operations. It is NOT
// sound for a system offering multi-key transactions, and this tool does not
// claim to check those.
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
// admits a linearization.
//
// This is the classical Wing-Gong backtracking search, pruned two ways: an
// operation can only be placed next if every operation real-time-forced to
// precede it has already been placed, and a (remaining-set, resulting-state)
// pair already shown to have no completion is never re-explored.
//
// The search runs over the whole per-key history at once. An earlier draft cut
// the history into real-time segments and threaded a single model state
// between them, which is unsound: a segment containing two concurrent writes
// ends in two possible states, and a later read may be explained by either.
// Result.Segments still reports the segment count as a concurrency statistic,
// but nothing depends on it for correctness.
//
// The search is exponential in the number of real-time-overlapping operations
// in the worst case, which is inherent to the problem. That is what
// Options.Budget is for, and why exceeding it yields Unknown, not a pass.
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

// segment counts the points where real-time order forces a complete
// separation: every operation before the cut returned strictly before every
// operation after it was called. Reported as a statistic only. An in-doubt
// operation blocks every later cut, since it has no Return and may still be
// pending arbitrarily far in the future.
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

// bitset is a growable set of operation indices. Using this rather than a
// single uint64 is what removes the 63-operations-per-check limit the original
// implementation had. That limit forced callers to truncate histories, and
// truncating a history is the bug class this package exists to avoid.
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) has(i int) bool { return b[i/64]&(1<<uint(i%64)) != 0 }
func (b bitset) set(i int)      { b[i/64] |= 1 << uint(i%64) }
func (b bitset) clear(i int)    { b[i/64] &^= 1 << uint(i%64) }
func (b bitset) clone() bitset  { return append(bitset(nil), b...) }

// key renders the bitset as a map key. unsafe.String over the backing array
// avoids allocating on every memo probe; the string is only used as a map key,
// and the bitset is cloned before it is stored.
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

	// nRequired is how many operations a linearization must account for.
	// In-doubt operations are excluded: the client never learned whether they
	// took effect, so a linearization that leaves one out is just as faithful
	// an explanation of what happened as one that includes it.
	nRequired := 0
	for _, op := range ops {
		if !op.InDoubt {
			nRequired++
		}
	}

	// succ[j] lists the operations j is real-time-forced to precede, meaning j
	// returned strictly before they were called. pending[i] counts how many of
	// i's forced predecessors are still unplaced, which makes eligibility an
	// O(1) test instead of an O(n) scan at every step. Without it a long
	// sequential history costs O(n^3) to walk and the checker stalls on
	// histories it should handle instantly.
	//
	// The comparison is strict (Before, not "<=") deliberately. Two operations
	// whose recorded instants are equal are treated as concurrent rather than
	// forced into an order, because a wall clock's resolution is finite: two
	// time.Now() calls with nothing but a fast function call between them can
	// read back equal. A non-strict check breaks on exactly that case, since
	// two tied ops each look forced to precede the other, so neither is ever
	// eligible and a real linearization is missed. Treating ties as concurrent
	// only ever widens the search, so it can never turn a genuine violation
	// into a false pass.
	succ := make([][]int, n)
	pending := make([]int, n)
	for j := 0; j < n; j++ {
		if ops[j].InDoubt {
			// j never returned, so nothing forces it to precede anything: it
			// may still be pending right now. Its interval runs to the end of
			// time.
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

// minimize shrinks a violating history to a sub-history that still fails.
// Dropping operations can only make a history easier to linearize, so anything
// that still fails after a drop is strictly stronger evidence.
//
// It works in chunks, largest first, the way delta debugging does: try to
// delete half the history, then quarters, and so on down to single
// operations. A one-at-a-time pass over a 300-operation history costs tens of
// thousands of searches and does not finish; chunk removal gets to the same
// place in a fraction of that because the first few deletions do most of the
// work.
//
// Two rules keep the output honest rather than merely small:
//
//   - Well-formedness. Without it the minimizer deletes the Put that a Get
//     observed and reports a one-operation "violation" in which a value
//     appeared from nowhere. That is technically unlinearizable and says
//     nothing whatever about the system.
//   - Deletes are only removable once every remaining read has finished.
//     Removing an earlier one turns a Get that legitimately found nothing into
//     a fabricated contradiction with some earlier Put.
func minimize(ops []Op, deadline time.Time) []Op {
	// Minimization must not eat the budget the verdict itself needed. The
	// verdict is already decided at this point, so a partially minimized
	// history is an acceptable outcome; an unminimized one is still correct,
	// just harder to read.
	if budget := time.Until(deadline); budget > minimizeBudget {
		deadline = time.Now().Add(minimizeBudget)
	}

	cur := append([]Op(nil), ops...)
	for chunk := len(cur) / 2; chunk >= 1; chunk /= 2 {
		for progress := true; progress; {
			progress = false
			for i := 0; i+1 <= len(cur) && chunk < len(cur); i++ {
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
// yields evidence that is still about the system rather than about the
// deletion.
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
// still present in the history. A history failing this is unlinearizable for a
// trivial reason and proves nothing about the system.
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
// minimized history: a violation claim without a printable, reproducible
// history is not a claim this tool is willing to make.
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
