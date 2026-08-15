// Package lincheck is a single-key linearizability checker and fault-injection
// harness for external key-value systems.
//
// It is the checker from the quorum project, generalized so that it depends on
// nothing but the standard library, plus the harness machinery needed to point
// it at a real system running in real processes.
//
// Two properties of this package matter more than the search algorithm itself,
// because both were bugs before they were features:
//
//  1. An operation whose outcome the client never learned is INDETERMINATE, not
//     failed. See Op.InDoubt.
//
//  2. A harness that silently records less than it claims is worse than no
//     harness. Nothing here truncates a history, and every run reports coverage
//     numbers that a caller is expected to assert on. See Coverage.
package lincheck

import (
	"fmt"
	"sync"
	"time"
)

// OpType names what an Op did to its key.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpGet
)

func (t OpType) String() string {
	switch t {
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	case OpGet:
		return "Get"
	default:
		return "Unknown"
	}
}

// Op is one client operation against one key, with the real-time interval it
// was observed to span and, for a Get, the value the client actually saw.
//
// Call and Return must come from the same clock the caller used to invoke the
// operation and observe its result. The checker only ever compares them to
// each other.
type Op struct {
	Client int    // arbitrary id distinguishing concurrent callers; used for reporting only
	Node   string // which node of the target the client was talking to; reporting only
	Key    string
	Type   OpType
	Value  []byte // the value written, for OpPut

	Call   time.Time
	Return time.Time

	// ResultFound/ResultValue are only meaningful for OpGet: what the client
	// actually observed. A Put or Delete is checked for whether it could have
	// happened, not for any return value.
	ResultFound bool
	ResultValue []byte

	// InDoubt marks an operation the client submitted but never learned the
	// outcome of: it timed out, or the connection broke, and the request may
	// still be sitting in the system waiting to commit. This is the ":info"
	// case in Jepsen's vocabulary, and it is not an edge case: it is the
	// normal fate of a write sent to a node that is about to be partitioned
	// away, which is exactly what a fault-injected run produces on purpose.
	//
	// An in-doubt operation is checked as OPTIONAL. A linearization may place
	// it anywhere at or after its Call, or leave it out entirely, because both
	// are things that could really have happened. Dropping such an operation
	// instead is unsound in both directions: a write that timed out and then
	// committed makes a later Get look impossible (a FALSE violation), and a
	// write that timed out and then committed over a value can HIDE a genuine
	// violation. Return is ignored for an in-doubt operation, since there was
	// no return.
	InDoubt bool

	// Err is the error the client saw, if any. Reporting only.
	Err string
}

func (op Op) String() string {
	doubt := ""
	if op.InDoubt {
		doubt = " [in doubt: outcome never observed"
		if op.Err != "" {
			doubt += ": " + op.Err
		}
		doubt += "]"
	}
	node := ""
	if op.Node != "" {
		node = "@" + op.Node
	}
	switch op.Type {
	case OpGet:
		return fmt.Sprintf("client%d%s: Get(%q) -> found=%v value=%q%s", op.Client, node, op.Key, op.ResultFound, op.ResultValue, doubt)
	case OpPut:
		return fmt.Sprintf("client%d%s: Put(%q, %q)%s", op.Client, node, op.Key, op.Value, doubt)
	default:
		return fmt.Sprintf("client%d%s: Delete(%q)%s", op.Client, node, op.Key, doubt)
	}
}

// Recorder collects Ops from any number of concurrent callers, the shape a
// chaos run's client goroutines naturally produce, and hands back a stable
// snapshot once the run is over.
type Recorder struct {
	mu  sync.Mutex
	ops []Op

	// givenUp counts operations a client wanted to perform but never managed
	// to even submit, because every node refused it for the whole retry
	// window. These never enter the history, so without counting them a run
	// that talked to nothing at all looks identical to a clean run.
	givenUp int
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Record appends one operation. Safe for concurrent use.
func (r *Recorder) Record(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// GaveUp records that a client abandoned an intended operation without ever
// submitting it successfully.
func (r *Recorder) GaveUp() {
	r.mu.Lock()
	r.givenUp++
	r.mu.Unlock()
}

// Ops returns a copy of every operation recorded so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

// GivenUp returns the number of abandoned operations.
func (r *Recorder) GivenUp() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.givenUp
}

// FormatHistory renders a history in call order, one op per line, with
// timestamps relative to the first call. This is the form a violation report
// must be printable in: it has to be readable and it has to be complete.
func FormatHistory(ops []Op) string {
	if len(ops) == 0 {
		return "(empty history)\n"
	}
	base := ops[0].Call
	for _, op := range ops {
		if op.Call.Before(base) {
			base = op.Call
		}
	}
	sorted := append([]Op(nil), ops...)
	sortByCall(sorted)
	out := ""
	for i, op := range sorted {
		ret := "        n/a"
		if !op.InDoubt {
			ret = fmt.Sprintf("%11s", op.Return.Sub(base).Round(time.Microsecond))
		}
		out += fmt.Sprintf("  %2d  call=%11s ret=%s  %s\n",
			i, op.Call.Sub(base).Round(time.Microsecond), ret, op)
	}
	return out
}

func sortByCall(ops []Op) {
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && ops[j].Call.Before(ops[j-1].Call); j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
}
