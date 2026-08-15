# RESULTS: etcd

**Verdict: no linearizability violation found.** 7200 operations across 10
seeded fault-injection schedules against a real 3-node cluster. This is the
expected outcome and it is reported as such.

## Consistency model etcd documents

etcd documents linearizable reads as the default and describes the mechanism:

> "etcd ensures linearizability for all other operations by default."

> "Linearizable requests ... go through a quorum of cluster members for
> consensus before serving."

(etcd documentation, `Documentation/learning/api_guarantees.md`.)

Serializable reads are the documented opt-out that may return stale data. **This
harness never sets `serializable`**, so every read below is a linearizable read.
Checking the serializable path against linearizability would flag documented
behavior, which is not a finding.

## Exact configuration

Image `gcr.io/etcd-development/etcd:v3.5.17`, Docker daemon 29.6.2, three
containers on a user-defined bridge network `lincheck-etcd`, each with its client
port published on loopback (23791, 23792, 23793).

Per-member flags:

```
--data-dir /etcd-data
--listen-client-urls http://0.0.0.0:2379
--advertise-client-urls http://<member>:2379
--listen-peer-urls http://0.0.0.0:2380
--initial-advertise-peer-urls http://<member>:2380
--initial-cluster lincheck-etcd-0=http://lincheck-etcd-0:2380,
                  lincheck-etcd-1=http://lincheck-etcd-1:2380,
                  lincheck-etcd-2=http://lincheck-etcd-2:2380
--initial-cluster-state new
--initial-cluster-token lincheck
--heartbeat-interval 100
--election-timeout 1000
```

The election timings are shortened from the defaults on purpose: with the stock
values a leader change often does not complete inside a schedule, so the fault
is injected and recovered without the cluster ever being meaningfully disturbed.

Client access is the v3 gRPC-gateway JSON API over plain `net/http`:
`/v3/kv/range`, `/v3/kv/put`, `/v3/kv/deleterange`. Keep-alives are disabled so
a pooled connection to a partitioned node cannot turn a clean rejection into a
timeout. `etcdserver: no leader` is classified as a definite rejection;
`etcdserver: request timed out` is classified as indeterminate, because that
request may well already be in the raft log.

The cluster is destroyed and recreated from scratch before every schedule, and
`Setup` does not return until a write and a delete both succeed.

## Exact command

```
go run ./cmd/lincheck -target etcd -schedules 10 -seed 1 -clients 6 -ops 120 \
  -keys k0,k1,k2,k3,k4,k5 -read-ratio 0.5 -think 200ms -budget 60s \
  -fault-hold 4s -fault-gap 3s
```

## Fault schedule

Fault set, nine faults, chosen uniformly at random with the schedule seed:

```
pause(lincheck-etcd-N)                       SIGSTOP the member, alive but silent
kill(lincheck-etcd-N)                        SIGKILL, restarted on recovery
partition(lincheck-etcd-N from lincheck-etcd) network disconnect, reconnect on recovery
```

for N in 0, 1, 2. First fault at 1s into the schedule, each held 4s, 3s of
health between faults, repeating until the clients finish.

Schedule 1 (seed 1) as an example of what one actually looked like:

```
    1s    inject  kill(lincheck-etcd-0)
 5.709s   recover kill(lincheck-etcd-0)
 8.710s   inject  partition(lincheck-etcd-1 from lincheck-etcd)
13.255s   recover partition(lincheck-etcd-1 from lincheck-etcd)
16.256s   inject  kill(lincheck-etcd-1)
20.993s   recover kill(lincheck-etcd-1)
```

## Counts

| | |
|---|---|
| Schedules | 10 (seeds 1 through 10) |
| Clients per schedule | 6, spread round-robin across the 3 members |
| Operations attempted per schedule | 720 |
| Operations recorded, total | 7200 |
| Completed (definite outcome) | 7055 |
| In doubt (outcome never observed) | 145 |
| Abandoned without being submitted | 0 |
| Faults injected, total | 34 (13 kill, 13 partition, 8 pause) |
| Keys | 6 |
| Schedule wall-clock | 23 to 26s each |
| Verdicts | 10 LINEARIZABLE, 0 VIOLATION, 0 UNKNOWN, 0 harness failures |

Coverage for schedule 1, as an example of the floor being cleared with room:

```
ops=720 completed=706 (get=351 put=319) in-doubt=14 given-up=0
reads-of-run-values=294 keys=6 min-ops-per-key=108 faults=3 duration=23.439s
```

294 of 351 reads returned a value written during that run, which is the number
that establishes the harness was actually exercising the store.

## Result

No violation. No key in any of the 60 per-key checks (10 schedules x 6 keys)
failed to linearize, and no check hit its 60s budget, so there are no UNKNOWN
verdicts hiding behind the clean result.

There is no violating history to print, because there was no violation.

## What this does and does not establish

It establishes that etcd's default read path held linearizability across 7200
operations under repeated leader kills, process freezes, and network partitions,
and that the harness recorded enough real work in each schedule to make that
statement meaningful.

It does not establish that etcd is linearizable. Ten schedules is a small search
of a large space, and a clean result from a bounded random run is weak evidence
in the direction of correctness by construction. It also says nothing about
watch, leases, multi-key transactions, or serializable reads, none of which this
tool checks.

The full run log is in `runs/etcd.txt`.
