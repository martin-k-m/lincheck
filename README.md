# lincheck

A single-key linearizability checker and fault-injection harness you can point at
someone else's system.

The checker is the Wing-Gong search, the same algorithm porcupine implements and
etcd's own robustness suite uses. The algorithm is not the interesting part. The
interesting parts are the two soundness rules it enforces, both of which exist
because the earlier version of this checker got them wrong and produced results
that looked fine and were not.

## The two rules

**1. An operation whose outcome the client never learned is indeterminate, not
failed.**

A write that timed out may still commit. Modelling it as "did not happen"
produces false violations, and it can also hide real ones, because a write that
timed out and then committed over a value erases the evidence. lincheck records
these with `Op.InDoubt` and checks them as optional: a linearization may place
one anywhere at or after its call, or leave it out. This is Jepsen's `:info`
case, and it is the same problem etcd solves with its non-deterministic
fork-and-merge state model.

Both directions are regression tests: `TestInDoubtWriteDoesNotCauseFalseViolation`
and `TestInDoubtWriteDoesNotHideRealViolation`.

**2. A checker that silently checks nothing is worse than no checker.**

Two harness bugs produced runs that reported "pass" while testing almost
nothing: a per-key operation cap that truncated histories, and clients with no
retry backoff that burned their entire operation budget in milliseconds during a
leaderless window. Both are addressed structurally:

- There is no operation cap. The search uses a growable bitset, so nothing is
  ever truncated to fit a `uint64`.
- Every run measures `Coverage` and asserts it against a floor. A run that does
  not clear the floor is reported as `HARNESS FAILURE` and **no verdict is
  computed for it at all**. The single most useful number is
  `ReadsOfRunValues`: reads that observed a value this run wrote. A run whose
  reads all returned "not found" proves nothing about a store.
- A search that exhausts its time budget returns `UNKNOWN`, which is a third
  verdict, not a pass, and has its own exit code.

The coverage floor is not decorative. The first real etcd run failed it: all 60
operations completed in 87ms, before the first fault was injected. Without the
floor that run would have reported a clean pass against a cluster that was never
disturbed. That run's log was not kept, so those two numbers are from my notes
and are not reproducible from `runs/`.

## The adapter interface

This is the product. Three interfaces, eight methods.

```go
type Client interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Node() string
	Close() error
}

type Target interface {
	Name() string
	Setup(ctx context.Context) error
	Clients(ctx context.Context, n int) ([]Client, error)
	Faults() []Fault
	Teardown(ctx context.Context) error
}

type Fault interface {
	Name() string
	Inject(ctx context.Context) error
	Recover(ctx context.Context) error
}
```

Plus one optional interface, `Documented`, carrying the consistency model the
vendor's own documentation claims. It is optional in Go and not optional in
practice: a finding is only a finding relative to a documented promise.

The whole contract that is not visible in the signatures is the error contract
on `Client`, and it is where adapters get linearizability testing wrong:

```
nil error       the operation definitely happened
ErrRejected     the operation definitely did NOT happen
anything else   INDETERMINATE, recorded with InDoubt set
```

Indeterminate is the default. An adapter that cannot prove an operation was
rejected must not claim it was. This is also why the three adapters here speak
HTTP and RESP directly instead of using vendor client libraries: a client
library that transparently retries turns indeterminate outcomes into apparent
successes and quietly destroys the history.

## Fault injection

`dockerfault` drives the `docker` CLI against real containers: `pause` (SIGSTOP,
the classic slow node), `kill` plus restart, and `network disconnect` plus
reconnect. Shelling out is deliberate: it works against any image, needs no
client library, and the exact commands are printable, which matters because a
fault schedule that cannot be written down cannot be reproduced.

## Running it

`go test ./...` needs nothing but Go. Every target below needs a running Docker
daemon and pulls its own image on first use, because the adapters create and
destroy real containers.

```
go test ./...                                   # the checker's own regression suite
go run ./cmd/lincheck -target etcd -schedules 10
go run ./cmd/lincheck -target consul -schedules 10
go run ./cmd/lincheck -target redis -schedules 10 -ops 400 -think 30ms
```

Exit status: 0 clean, 1 violation, 2 inconclusive (coverage floor missed or
search budget exhausted). Inconclusive does not share an exit code with clean.

## Results

- [RESULTS-etcd.md](RESULTS-etcd.md) — clean
- [RESULTS-consul.md](RESULTS-consul.md) — clean
- [RESULTS-redis.md](RESULTS-redis.md) — violations found, exactly as Redis
  documents. This target exists to prove the checker detects, not to report a
  discovery.
- [RESULTS-hosted-service.md](RESULTS-hosted-service.md) — not run, and why.

Finding nothing against etcd and Consul is the expected outcome. Both are mature
systems whose documented guarantee is the one being checked, and a clean result
is a statement about the harness at least as much as about them.

## What this does not check

Single-key linearizability only. Multi-key transactions, watch streams, lease
semantics, and serializable reads are all out of scope, and a clean result here
says nothing about any of them. etcd's own robustness suite validates its watch
contract with nine separate hand-written validators, none of which are
linearizability checks; that is the right shape, and it is not what this is.

Nothing in this repository has been sent anywhere. No issue was filed, no
maintainer contacted, nothing pushed.

## Licence

MIT.
