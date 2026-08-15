package lincheck

import (
	"context"
	"errors"
)

// The adapter surface is the product here, so it is deliberately small: three
// interfaces, eight methods total, and one optional interface.
//
// To point lincheck at a new system you implement Target, hand back Clients,
// and list Faults. Nothing else about your system is visible to the harness.

// Client is one connection to the system under test, from the point of view of
// one simulated client. A Client is used from a single goroutine.
//
// Error contract, and this is the part that matters:
//
//	nil error          the operation definitely happened
//	ErrRejected        the operation definitely did NOT happen (the system
//	                   refused it before doing anything: "not the leader",
//	                   "no cluster leader", a connection refused before send)
//	any other error    INDETERMINATE. The harness records the operation with
//	                   InDoubt set.
//
// Indeterminate is the default for a reason. An adapter that cannot prove an
// operation was rejected must not claim it was. Claiming it produces false
// violations, and can also mask real ones.
type Client interface {
	// Get reads a key. found reports whether the key exists.
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	// Put writes a key.
	Put(ctx context.Context, key string, value []byte) error
	// Delete removes a key.
	Delete(ctx context.Context, key string) error
	// Node names the node this client is talking to, for reporting.
	Node() string
	// Close releases the connection.
	Close() error
}

// ErrRejected is returned by a Client when the system definitively refused an
// operation without performing it. Wrap it: fmt.Errorf("...: %w", ErrRejected).
var ErrRejected = errors.New("lincheck: operation definitively rejected, did not happen")

// Rejected reports whether err means the operation definitely did not happen.
func Rejected(err error) bool { return errors.Is(err, ErrRejected) }

// Target is a system under test: something that can be brought up, connected
// to, disrupted, and torn down.
type Target interface {
	// Name identifies the target in reports.
	Name() string
	// Setup brings the system to a healthy, empty state. It is called once per
	// schedule, so it must be idempotent enough to run repeatedly.
	Setup(ctx context.Context) error
	// Clients returns n clients. An implementation is expected to spread them
	// across nodes; which node each got is reported via Client.Node.
	Clients(ctx context.Context, n int) ([]Client, error)
	// Faults returns the faults that may be injected against this target. An
	// empty list means the run is a plain concurrency soak with no faults,
	// which is a legitimate but much weaker test, and the report says so.
	Faults() []Fault
	// Teardown stops the system.
	Teardown(ctx context.Context) error
}

// Fault is one injectable failure against real processes. Inject and Recover
// are always paired: the driver guarantees Recover is called for every Inject,
// including on abort.
type Fault interface {
	Name() string
	Inject(ctx context.Context) error
	Recover(ctx context.Context) error
}

// Documented is an optional interface a Target may implement to carry the
// consistency model its own documentation claims, so a report cannot be
// written without it. A finding is only a finding relative to a documented
// promise.
type Documented interface {
	// DocumentedModel is a short statement of what the vendor documents,
	// with a citation.
	DocumentedModel() string
}

// FuncFault builds a Fault from two closures.
type FuncFault struct {
	FaultName string
	On        func(context.Context) error
	Off       func(context.Context) error
}

func (f FuncFault) Name() string { return f.FaultName }
func (f FuncFault) Inject(ctx context.Context) error {
	if f.On == nil {
		return nil
	}
	return f.On(ctx)
}
func (f FuncFault) Recover(ctx context.Context) error {
	if f.Off == nil {
		return nil
	}
	return f.Off(ctx)
}
