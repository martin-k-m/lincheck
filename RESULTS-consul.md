# RESULTS: Consul

**Verdict: no linearizability violation found.** 7200 operations across 10
seeded fault-injection schedules against a real 3-server cluster. This is the
expected outcome and it is reported as such.

## Consistency model Consul documents

Consul documents three read consistency modes for the KV store. The one this
harness uses is `consistent`:

> "This mode is strongly consistent without caveats. It requires that a leader
> verify with a quorum of peers that it is still leader. This introduces an
> additional round-trip to all server nodes."

The `default` mode is documented as able to serve stale data:

> "in a very rare failure scenario ... the old leader may service some reads"

(Consul documentation, API consistency modes.) Writes go through Raft.

**Every read below sets `?consistent=true`.** Checking the `default` mode
against linearizability would flag documented behavior, which is not a finding.

## Exact configuration

Image `hashicorp/consul:1.20`, Docker daemon 29.6.2, three server agents on a
user-defined bridge network `lincheck-consul`, HTTP API published on loopback
(28501, 28502, 28503).

Per-agent flags:

```
agent -server -node=<name> -bootstrap-expect=3
      -client=0.0.0.0 -bind=0.0.0.0 -data-dir=/consul/data
      -hcl='performance { raft_multiplier = 1 }'
      -retry-join=<the other members>
```

`raft_multiplier = 1` is Consul's own documented low-latency setting. The stock
value of 5 scales the Raft timeouts up by 5x, which puts a leader election
outside the length of a schedule and means a fault would be injected and
recovered without the cluster being meaningfully disturbed.

Client access is the HTTP KV API directly: `GET /v1/kv/<key>?consistent=true`,
`PUT /v1/kv/<key>`, `DELETE /v1/kv/<key>`. Keep-alives are disabled. A response
body of `No cluster leader` is classified as a definite rejection, and a `PUT`
returning the literal `false` is classified as a definite rejection; every other
failure is classified as indeterminate.

The cluster is destroyed and recreated before every schedule, and `Setup` does
not return until a write, a read-back, and a delete all succeed.

## Exact command

```
go run ./cmd/lincheck -target consul -schedules 10 -seed 1 -clients 6 -ops 120 \
  -keys k0,k1,k2,k3,k4,k5 -read-ratio 0.5 -think 200ms -budget 60s \
  -fault-hold 4s -fault-gap 3s
```

## Fault schedule

Fault set, nine faults, chosen uniformly at random with the schedule seed:

```
pause(lincheck-consul-N)                          SIGSTOP the server
kill(lincheck-consul-N)                           SIGKILL, restarted on recovery
partition(lincheck-consul-N from lincheck-consul) network disconnect, reconnect on recovery
```

for N in 0, 1, 2. First fault at 1s, held 4s, 3s of health between faults.

Schedule 1 (seed 1):

```
 1.001s   inject  kill(lincheck-consul-0)
 5.674s   recover kill(lincheck-consul-0)
 8.674s   inject  partition(lincheck-consul-1 from lincheck-consul)
13.225s   recover partition(lincheck-consul-1 from lincheck-consul)
16.225s   inject  kill(lincheck-consul-1)
21.003s   recover kill(lincheck-consul-1)
24.004s   inject  kill(lincheck-consul-0)
25.821s   recover kill(lincheck-consul-0)
```

## Counts

| | |
|---|---|
| Schedules | 10 (seeds 1 through 10) |
| Clients per schedule | 6, spread round-robin across the 3 servers |
| Operations attempted per schedule | 720 |
| Operations recorded, total | 7200 |
| Completed (definite outcome) | 6941 |
| In doubt (outcome never observed) | 259 |
| Abandoned without being submitted | 0 |
| Faults injected, total | 37 (14 kill, 14 partition, 9 pause) |
| Keys | 6 |
| Schedule wall-clock | 21.318s to 31.549s each |
| Verdicts | 10 LINEARIZABLE, 0 VIOLATION, 0 UNKNOWN, 0 harness failures |

Coverage for schedule 1:

```
ops=720 completed=688 (get=342 put=311) in-doubt=32 given-up=0
reads-of-run-values=283 keys=6 min-ops-per-key=108 faults=4 duration=25.821s
```

Consul produced noticeably more indeterminate operations than etcd under the
same schedule shape (259 against 145). That is a difference in how the two
systems behave when a client's request reaches a server that is losing or has
lost leadership, not a correctness signal in either direction. It is worth
noting only because a checker that treated those 259 operations as "did not
happen" would have had 259 chances to invent a violation.

## Result

No violation. No key in any of the 60 per-key checks failed to linearize, and no
check hit its 60s budget, so there are no UNKNOWN verdicts behind the clean
result.

There is no violating history to print, because there was no violation.

## What this does and does not establish

It establishes that Consul's `consistent` read mode held linearizability across
7200 operations under repeated leader kills, freezes, and partitions.

It does not establish that Consul is linearizable, and it says nothing at all
about the `default` or `stale` read modes, sessions, locks, or watches. It is
also a KV-only result: Consul's service catalog is a different subsystem with
different guarantees and is untouched here.

The full run log is in `runs/consul.txt`.
