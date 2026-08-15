# RESULTS: Redis with replication

**Verdict: violations found, in 10 of 10 schedules. This is not a finding about
Redis.** It is the harness validating itself against a system whose own
documentation says it will lose acknowledged writes on failover. If lincheck had
reported this configuration clean, lincheck would be broken.

## Consistency model Redis documents

Redis documents its replication as asynchronous and states plainly that
acknowledged writes can be lost:

> "Redis uses by default asynchronous replication, which being low latency and
> high performance, is the natural replication mode for the vast majority of
> Redis use cases. However, Redis replicas asynchronously acknowledge the amount
> of data they receive periodically."

> "Redis Cluster is not able to guarantee strong consistency."

(Redis documentation, replication and the Redis Cluster specification.)

Losing writes here is correct, documented behavior. What follows is evidence
about the checker, not about Redis.

## Exact configuration

Image `redis:7-alpine`, Docker daemon 29.6.2. Two containers, one primary and
one replica, `redis-server --save '' --appendonly no` (no persistence, so a
killed node comes back empty and cannot mask the loss).

Two Docker networks, and this detail is load-bearing:

- `lincheck-redis-client` carries the published loopback ports (26379, 26380).
- `lincheck-redis-repl` carries replication, with each container joined under
  the alias `<name>-repl`.

`REPLICAOF lincheck-redis-0-repl 6379` targets the repl-network alias, not the
container name. Docker gives every container its own name as a DNS alias on
every network it joins, so `REPLICAOF <container-name>` resolves over the client
network and the replication connection survives a disconnect from the
replication network. That is not hypothetical: with the container name as the
target, three schedules of 360 operations came back LINEARIZABLE because
replication never actually broke.

`Setup` does not return until the replica reports `master_link_status:up`.
`REPLICAOF` returns `OK` immediately and the link takes seconds to establish; a
run that skipped this wait was partitioning a link that had never come up.

Clients speak a minimal RESP subset over TCP, one fresh connection per
operation, always to whichever node is currently primary. A real Redis client
library would reconnect and retry transparently and destroy the history.
`READONLY` and `LOADING` replies are classified as definite rejections;
everything else is indeterminate.

## Exact command

```
go run ./cmd/lincheck -target redis -schedules 10 -seed 1 -clients 3 -ops 400 \
  -keys k0,k1,k2,k3 -read-ratio 0.6 -think 30ms -budget 60s \
  -fault-hold 6s -fault-gap 6s
```

## Exact fault schedule

One fault, injected once per schedule at 1s:

```
failover(isolate replica 2s, kill primary, promote replica)
  1. docker network disconnect -f lincheck-redis-repl <replica>
  2. wait 2s while clients keep writing to the primary
  3. docker kill --signal KILL <primary>
  4. docker network connect --alias <replica>-repl lincheck-redis-repl <replica>
  5. REPLICAOF NO ONE on the replica; clients are pointed at it
recovery:
  docker start <old primary>; REPLICAOF <new primary>-repl 6379
```

Three earlier versions of this fault found nothing, and each failure was a
harness problem rather than a Redis result:

1. **Kill the primary and promote the replica, no partition.** Over loopback at
   a few hundred operations per second, replication is so far ahead that the
   replica already has every acknowledged write. Three schedules of 1000
   operations: LINEARIZABLE. A true statement about that run and a useless test.
2. **Partition the replica using the container name.** Replication resolved over
   the client network and never broke. Three schedules of 360 operations:
   LINEARIZABLE.
3. **Correct partition, but schedules that ended 1s after the failover.** With
   120 operations per client at 30ms think time a schedule lasts about 4.2s, and
   the failover completes at about 3.2s, so almost no read ever landed on the
   promoted replica. Ten schedules: LINEARIZABLE. Raising to 400 operations per
   client extends each schedule to about 7.8s and gives roughly 4.5s of
   post-failover traffic, which is when the evidence appears.

Each of those is a case of the harness testing less than it claimed. None of
them was reported as a pass in this document, and the reason they were caught is
that a clean result against Redis was known in advance to be wrong.

## Counts

| | |
|---|---|
| Schedules | 10 (seeds 1 through 10) |
| Clients per schedule | 3 |
| Operations attempted per schedule | 1200 |
| Operations recorded, total | 12000 |
| Completed (definite outcome) | 11763 |
| In doubt (outcome never observed) | 237 |
| Abandoned without being submitted | 0 |
| Faults injected, total | 10 (one failover per schedule) |
| Keys | 4 |
| Schedule wall-clock | about 7.8s each |
| Verdicts | 10 of 10 schedules VIOLATION; 26 of 40 per-key checks VIOLATION |
| UNKNOWN verdicts, harness failures | 0 |

Coverage for schedule 1:

```
ops=1200 completed=1171 (get=708 put=425) in-doubt=29 given-up=0
reads-of-run-values=642 keys=4 min-ops-per-key=285 faults=1 duration=7.936s
```

## Violating history, minimized, printed in full

The clearest one, schedule 1 (seed 1), key `k0`. The full per-key history is 315
operations; the minimizer reduced it to three, and those three are sufficient on
their own. Timestamps are relative to the first operation in the printed
history.

```
key "k0": VIOLATION (315 ops, 146 segments, 30ms)
  minimized violating history (3 ops, no linearization exists):
     0  call=         0s ret=    1.032ms  client1@lincheck-redis-0: Put("k0", "c1-n70-s1")
     1  call=  398.411ms ret=  400.145ms  client2@lincheck-redis-0: Delete("k0")
     2  call=   2.91682s ret=  2.918381s  client2@lincheck-redis-1: Get("k0") -> found=true value="c1-n70-s1"
```

Read it as three facts:

1. `lincheck-redis-0`, the primary, acknowledged writing `c1-n70-s1` to `k0`.
2. 398ms later the same primary acknowledged deleting `k0`. That delete returned
   successfully, so any correct linearization must place it after the write.
3. 2.5s later, after the failover, `lincheck-redis-1` returned the deleted value.
   The delete was acknowledged and strictly precedes this read in real time, so
   no ordering of these three operations explains it. The value came back from
   the dead.

The write and the delete were both acknowledged during the window in which the
replica was cut off from replication. The replica was then promoted with a
dataset predating both, so the delete is gone and the old value is live again.
This is exactly the behavior Redis documents.

A second example, schedule 1, key `k1`, showing the same shape with a longer
chain of acknowledged deletes all erased at once:

```
key "k1": VIOLATION (303 ops, 139 segments, 43ms)
  minimized violating history (9 ops, no linearization exists):
     0  call=         0s ret=    1.107ms  client1@lincheck-redis-0: Delete("k1")
     1  call=  791.056ms ret=   792.68ms  client2@lincheck-redis-0: Put("k1", "c2-n55-s1")
     2  call=  957.308ms ret=  958.867ms  client0@lincheck-redis-0: Delete("k1")
     3  call=   1.41612s ret=  1.417715s  client0@lincheck-redis-0: Delete("k1")
     4  call=  1.507149s ret=  1.508193s  client0@lincheck-redis-0: Delete("k1")
     5  call=  2.025782s ret=  2.028171s  client1@lincheck-redis-0: Delete("k1")
     6  call=   2.76336s ret=  2.765848s  client1@lincheck-redis-0: Delete("k1")
     7  call=  2.817927s ret=  2.821361s  client2@lincheck-redis-0: Delete("k1")
     8  call=  3.814584s ret=  3.817275s  client0@lincheck-redis-1: Get("k1") -> found=true value="c2-n55-s1"
```

Six acknowledged deletes stand between the write and the read, all of them on
the old primary, and the promoted replica returns the written value anyway.

Both are fully reproducible: same image, same command, same seed.

## What this establishes

That lincheck detects a real linearizability violation against a real
distributed system, produces a minimized history small enough to read, and
reaches the same conclusion the vendor's documentation already states. Combined
with `TestCorrectReferenceStorePasses` (a genuinely linearizable in-memory store
comes back clean in 10 of 10 seeds), the clean etcd and Consul results are
results rather than silence.

The full run log is in `runs/redis.txt`.
