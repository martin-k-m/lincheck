// Package lincheck is a single-key linearizability checker and fault-injection
// harness for external key-value systems, depending on nothing but the
// standard library.
//
// Two properties matter more than the search algorithm, and both were bugs
// before they were features: an operation whose outcome the client never
// learned is INDETERMINATE, not failed (see Op.InDoubt), and nothing here
// truncates a history or reports a verdict for a run that measured too little
// (see Coverage). README.md has the rationale.
package lincheck

import (
	"fmt"
	"sort"
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
	// outcome of: it timed out or the connection broke, and it may still be
	// sitting in the system waiting to commit. Jepsen's ":info" case, and the
	// normal fate of a write to a node about to be partitioned away.
	//
	// Such an operation is checked as OPTIONAL: a linearization may place it
	// anywhere at or after its Call, or leave it out, because both really
	// could have happened. Dropping it instead is unsound in both directions.
	// A write that timed out and then committed makes a later Get look
	// impossible (a FALSE violation), and one that committed over a value can
	// HIDE a genuine violation. Return is ignored when this is set.
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

// Recorder collects Ops from any number of concurrent callers and hands back a
// stable snapshot once the run is over.
type Recorder struct {
	mu  sync.Mutex
	ops []Op

	// givenUp counts operations never submitted at all, because every node
	// refused them for the whole retry window. They never enter the history,
	// so without this count a run that talked to nothing looks clean.
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
// timestamps relative to the first call.
func FormatHistory(ops []Op) string {
	if len(ops) == 0 {
		return "(empty history)\n"
	}
	sorted := append([]Op(nil), ops...)
	sortByCall(sorted)
	base := sorted[0].Call
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

// sortByCall must be stable: ties are treated as concurrent everywhere else,
// so their relative order has to stay as recorded.
func sortByCall(ops []Op) {
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call.Before(ops[j].Call) })
}
