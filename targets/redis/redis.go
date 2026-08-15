// Package redis is a lincheck adapter for a Redis primary with one
// asynchronously replicated replica, plus a failover fault.
//
// This target exists to VALIDATE THE CHECKER, not to find a bug. Redis
// documents that its replication is asynchronous and that acknowledged writes
// can be lost when a replica is promoted. If lincheck reports this
// configuration clean, lincheck is broken. That is the entire point of
// including it.
//
// The client talks a minimal subset of RESP directly over TCP. A real Redis
// client library would reconnect and retry transparently, which would turn
// indeterminate outcomes into apparent successes and destroy the history.
package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/martin-k-m/lincheck"
	"github.com/martin-k-m/lincheck/dockerfault"
)

// Image is the Redis image under test.
const Image = "redis:7-alpine"

const (
	// Two networks, on purpose. replNetwork carries replication between the
	// two nodes; clientNetwork carries nothing but keeps the container
	// reachable on its published port. Disconnecting a node from replNetwork
	// alone cuts replication while leaving the node fully usable by clients,
	// which is the fault that matters here.
	// The alias on replNetwork is deliberately NOT the container name. Docker
	// gives every container its own name as a DNS alias on every network it
	// joins, so REPLICAOF <container-name> would resolve over the client
	// network and the replication connection would survive a disconnect from
	// the replication network. That is not a hypothetical: with the container
	// name as the target, three schedules of 360 operations with a 2s replica
	// partition came back LINEARIZABLE because replication never actually
	// broke. Using a repl-network-only alias is what makes the partition real.
	replAliasSuffix = "-repl"

	replNetwork   = "lincheck-redis-repl"
	clientNetwork = "lincheck-redis-client"
	prefix        = "lincheck-redis-"
)

type Node struct {
	Name string
	Port int
}

// Pair is a two-node Redis primary/replica deployment as a lincheck.Target.
type Pair struct {
	Nodes []Node

	mu      sync.RWMutex
	primary int // index into Nodes
}

func New() *Pair {
	return &Pair{
		Nodes: []Node{
			{Name: prefix + "0", Port: 26379},
			{Name: prefix + "1", Port: 26380},
		},
	}
}

func (p *Pair) Name() string { return "Redis 7 (primary + async replica, Docker)" }

func (p *Pair) DocumentedModel() string {
	return "Redis documents its replication as asynchronous and explicitly states that " +
		"acknowledged writes can be lost: \"Redis uses by default asynchronous replication, " +
		"which being low latency and high performance, is the natural replication mode for " +
		"the vast majority of Redis use cases. However, Redis replicas asynchronously " +
		"acknowledge the amount of data they receive periodically ... Redis Cluster is not " +
		"able to guarantee strong consistency\" (Redis docs, operate/oss_and_stack/management/" +
		"replication and Redis Cluster specification). This target is therefore NOT expected " +
		"to be linearizable, and is included to demonstrate that this checker detects a real " +
		"violation rather than to report one as a discovery."
}

// Primary returns the address of the node currently acting as primary.
func (p *Pair) primaryNode() Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Nodes[p.primary]
}

func (p *Pair) Setup(ctx context.Context) error {
	_ = p.Teardown(ctx)
	for _, nw := range []string{replNetwork, clientNetwork} {
		if _, err := dockerfault.Run(ctx, "network", "create", nw); err != nil {
			return err
		}
	}
	for _, n := range p.Nodes {
		args := []string{
			"run", "-d", "--name", n.Name, "--network", clientNetwork,
			"-p", fmt.Sprintf("127.0.0.1:%d:6379", n.Port),
			Image, "redis-server", "--save", "", "--appendonly", "no",
		}
		if _, err := dockerfault.Run(ctx, args...); err != nil {
			return err
		}
		if _, err := dockerfault.Run(ctx, "network", "connect", "--alias", n.Name+replAliasSuffix, replNetwork, n.Name); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.primary = 0
	p.mu.Unlock()

	if err := dockerfault.WaitHealthy(ctx, 60*time.Second, func(ctx context.Context) error {
		for _, n := range p.Nodes {
			c, err := dial(n, 2*time.Second)
			if err != nil {
				return err
			}
			_, err = c.cmd(ctx, "PING")
			c.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Node 1 replicates node 0, asynchronously, which is the Redis default.
	c, err := dial(p.Nodes[1], 3*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.cmd(ctx, "REPLICAOF", p.Nodes[0].Name+replAliasSuffix, "6379"); err != nil {
		return err
	}
	if _, err := c.cmd(ctx, "FLUSHALL"); err != nil {
		// A replica refuses writes; FLUSHALL on the primary is what matters.
		_ = err
	}
	pc, err := dial(p.Nodes[0], 3*time.Second)
	if err != nil {
		return err
	}
	defer pc.Close()
	if _, err := pc.cmd(ctx, "FLUSHALL"); err != nil {
		return err
	}

	// Wait for the replication link to actually come up. REPLICAOF returns OK
	// immediately and the link takes a few seconds to establish; starting a
	// schedule before then means the replica holds nothing, the fault
	// partitions a link that was never up, and the run says nothing about
	// replication. This was not hypothetical: a run that skipped this wait
	// reported LINEARIZABLE against a configuration that cannot be.
	return dockerfault.WaitHealthy(ctx, 60*time.Second, func(ctx context.Context) error {
		c, err := dial(p.Nodes[1], 2*time.Second)
		if err != nil {
			return err
		}
		defer c.Close()
		rep, err := c.cmd(ctx, "INFO", "replication")
		if err != nil {
			return err
		}
		if !strings.Contains(rep.str, "master_link_status:up") {
			return fmt.Errorf("replication link not up yet")
		}
		return nil
	})
}

func (p *Pair) Teardown(ctx context.Context) error {
	for _, n := range p.Nodes {
		dockerfault.MustRun(ctx, "rm", "-f", "-v", n.Name)
	}
	dockerfault.MustRun(ctx, "network", "rm", replNetwork)
	dockerfault.MustRun(ctx, "network", "rm", clientNetwork)
	return nil
}

func (p *Pair) Clients(_ context.Context, n int) ([]lincheck.Client, error) {
	out := make([]lincheck.Client, n)
	for i := range out {
		out[i] = &Client{pair: p, id: i}
	}
	return out, nil
}

// Faults returns the failover fault.
//
// This is the documented data-loss scenario, reproduced deliberately and in a
// way that makes the loss certain rather than likely.
//
// A naive version of this fault, kill the primary and promote the replica,
// finds nothing: over loopback with a few hundred operations per second, Redis
// replication is so far ahead that the replica already has every acknowledged
// write by the time the primary dies. Three schedules of 1000 operations each
// with that fault came back LINEARIZABLE, which is a true statement about that
// run and a useless test.
//
// So the fault first cuts the replica off from the replication network. Writes
// during that window are acknowledged by the primary and provably never reach
// the replica. Then the primary is killed and the replica is promoted. Every
// write acknowledged during the partition window is now gone.
func (p *Pair) Faults() []lincheck.Fault {
	return []lincheck.Fault{failover{p: p, lag: 2 * time.Second}}
}

type failover struct {
	p *Pair
	// lag is how long the replica is cut off from replication before the
	// primary is killed. Writes acknowledged in this window are the ones that
	// are lost.
	lag time.Duration
}

func (f failover) Name() string {
	return fmt.Sprintf("failover(isolate replica %s, kill primary, promote replica)", f.lag)
}

func (f failover) Inject(ctx context.Context) error {
	f.p.mu.Lock()
	old := f.p.primary
	next := 1 - old
	f.p.mu.Unlock()

	// 1. Cut replication. The replica keeps its client network, so it stays
	//    reachable; it simply stops receiving the primary's write stream.
	if _, err := dockerfault.Run(ctx, "network", "disconnect", "-f", replNetwork, f.p.Nodes[next].Name); err != nil {
		return err
	}

	// 2. Let clients write. Everything acknowledged now exists only on the
	//    primary.
	select {
	case <-ctx.Done():
	case <-time.After(f.lag):
	}

	// 3. Kill the primary, taking those writes with it.
	if _, err := dockerfault.Run(ctx, "kill", "--signal", "KILL", f.p.Nodes[old].Name); err != nil {
		return err
	}

	// 4. Promote the replica and point clients at it.
	if _, err := dockerfault.Run(ctx, "network", "connect", "--alias", f.p.Nodes[next].Name+replAliasSuffix, replNetwork, f.p.Nodes[next].Name); err != nil {
		return err
	}
	c, err := dial(f.p.Nodes[next], 3*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.cmd(ctx, "REPLICAOF", "NO", "ONE"); err != nil {
		return err
	}
	f.p.mu.Lock()
	f.p.primary = next
	f.p.mu.Unlock()
	return nil
}

// Recover restarts the dead node as a replica of the new primary. The primary
// does not move back: a failover is not undone, it is followed by the old
// primary rejoining as a follower, which is what a real operator does.
func (f failover) Recover(ctx context.Context) error {
	f.p.mu.RLock()
	cur := f.p.primary
	f.p.mu.RUnlock()
	old := 1 - cur

	if _, err := dockerfault.Run(ctx, "start", f.p.Nodes[old].Name); err != nil {
		return err
	}
	return dockerfault.WaitHealthy(ctx, 30*time.Second, func(ctx context.Context) error {
		c, err := dial(f.p.Nodes[old], 2*time.Second)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.cmd(ctx, "REPLICAOF", f.p.Nodes[cur].Name+replAliasSuffix, "6379")
		return err
	})
}

// ---------------------------------------------------------------------------
// Minimal RESP client
// ---------------------------------------------------------------------------

type conn struct {
	c net.Conn
	r *bufio.Reader
}

func dial(n Node, timeout time.Duration) (*conn, error) {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", n.Port), timeout)
	if err != nil {
		return nil, err
	}
	return &conn{c: c, r: bufio.NewReader(c)}, nil
}

func (c *conn) Close() error { return c.c.Close() }

type reply struct {
	str    string
	isNil  bool
	errMsg string
}

func (c *conn) cmd(ctx context.Context, args ...string) (reply, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = c.c.SetDeadline(dl)
	} else {
		_ = c.c.SetDeadline(time.Now().Add(5 * time.Second))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.c.Write([]byte(b.String())); err != nil {
		return reply{}, err
	}
	return c.read()
}

func (c *conn) read() (reply, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return reply{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return reply{}, fmt.Errorf("redis: empty reply line")
	}
	switch line[0] {
	case '+':
		return reply{str: line[1:]}, nil
	case '-':
		return reply{errMsg: line[1:]}, nil
	case ':':
		return reply{str: line[1:]}, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return reply{}, err
		}
		if n < 0 {
			return reply{isNil: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(c.r, buf); err != nil {
			return reply{}, err
		}
		return reply{str: string(buf[:n])}, nil
	default:
		return reply{}, fmt.Errorf("redis: unsupported reply %q", line)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Client opens a fresh connection per operation against whichever node is
// currently primary. A fresh connection per operation is slow and deliberate:
// a pooled connection to a killed node produces an ambiguous failure whose
// timing is dominated by TCP, not by the system under test.
type Client struct {
	pair *Pair
	id   int
	node string
}

func (c *Client) Node() string {
	if c.node == "" {
		return "?"
	}
	return c.node
}
func (c *Client) Close() error { return nil }

func (c *Client) exec(ctx context.Context, args ...string) (reply, error) {
	n := c.pair.primaryNode()
	c.node = n.Name
	timeout := 2 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}
	conn, err := dial(n, timeout)
	if err != nil {
		// Never connected, so the command definitely did not run.
		return reply{}, fmt.Errorf("%v: %w", err, lincheck.ErrRejected)
	}
	defer conn.Close()
	rep, err := conn.cmd(ctx, args...)
	if err != nil {
		return reply{}, err
	}
	if rep.errMsg != "" {
		if strings.HasPrefix(rep.errMsg, "READONLY") || strings.HasPrefix(rep.errMsg, "LOADING") {
			// The node told us it refused the command.
			return reply{}, fmt.Errorf("%s: %w", rep.errMsg, lincheck.ErrRejected)
		}
		return reply{}, fmt.Errorf("redis: %s", rep.errMsg)
	}
	return rep, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	rep, err := c.exec(ctx, "GET", key)
	if err != nil {
		return nil, false, err
	}
	if rep.isNil {
		return nil, false, nil
	}
	return []byte(rep.str), true, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.exec(ctx, "SET", key, string(value))
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.exec(ctx, "DEL", key)
	return err
}
