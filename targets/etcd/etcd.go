// Package etcd is a lincheck adapter for a real 3-node etcd cluster running in
// Docker containers.
//
// It speaks etcd's v3 gRPC-gateway JSON API over plain net/http rather than
// linking clientv3. That is a deliberate trade: it keeps this repository free
// of etcd's dependency tree, and more importantly it keeps the adapter honest,
// because every request is visible as an HTTP call with explicit options
// rather than hidden behind a client library's retry and failover logic. A
// client library that transparently retries would turn indeterminate outcomes
// into apparent successes, which is precisely what must not happen here.
package etcd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/martin-k-m/lincheck"
	"github.com/martin-k-m/lincheck/dockerfault"
)

// Image is the etcd image under test.
const Image = "gcr.io/etcd-development/etcd:v3.5.17"

const (
	network = "lincheck-etcd"
	prefix  = "lincheck-etcd-"
)

// Node is one cluster member.
type Node struct {
	Name       string
	ClientPort int
}

// Cluster is a 3-node etcd cluster as a lincheck.Target.
type Cluster struct {
	Nodes []Node
	http  *http.Client
}

// New returns a 3-node cluster definition. Nothing is started until Setup.
func New() *Cluster {
	return &Cluster{
		Nodes: []Node{
			{Name: prefix + "0", ClientPort: 23791},
			{Name: prefix + "1", ClientPort: 23792},
			{Name: prefix + "2", ClientPort: 23793},
		},
		http: &http.Client{
			Transport: &http.Transport{
				// No connection reuse across faults: a pooled connection to a
				// partitioned node hangs until the OS notices, which turns a
				// clean rejection into a timeout and inflates the in-doubt
				// count for no reason.
				DisableKeepAlives:   true,
				DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
				TLSHandshakeTimeout: 2 * time.Second,
			},
		},
	}
}

func (c *Cluster) Name() string { return "etcd 3.5.17 (3-node cluster, Docker)" }

// DocumentedModel is what etcd's own documentation promises.
func (c *Cluster) DocumentedModel() string {
	return "etcd documents its key-value store as sequentially consistent and its default " +
		"reads as linearizable: \"etcd ensures linearizability for all other operations by " +
		"default\" and \"linearizable requests ... go through a quorum of cluster members for " +
		"consensus before serving\" (etcd docs, learning/api_guarantees.md). Serializable " +
		"reads are documented as the opt-out that may return stale data; this harness never " +
		"sets that flag, so every read below is a linearizable read."
}

// initialCluster is the --initial-cluster string, using container names as
// peer hostnames on the user-defined bridge network.
func (c *Cluster) initialCluster() string {
	var parts []string
	for _, n := range c.Nodes {
		parts = append(parts, fmt.Sprintf("%s=http://%s:2380", n.Name, n.Name))
	}
	return strings.Join(parts, ",")
}

// Setup destroys any previous cluster and brings up a fresh empty one. It
// returns only once a leader exists and a write succeeds: starting a schedule
// against a cluster that has not elected a leader records a history that
// proves nothing.
func (c *Cluster) Setup(ctx context.Context) error {
	_ = c.Teardown(ctx)

	if _, err := dockerfault.Run(ctx, "network", "create", network); err != nil {
		return err
	}
	for _, n := range c.Nodes {
		args := []string{
			"run", "-d", "--name", n.Name, "--network", network, "--network-alias", n.Name,
			"-p", fmt.Sprintf("127.0.0.1:%d:2379", n.ClientPort),
			Image, "/usr/local/bin/etcd",
			"--name", n.Name,
			"--data-dir", "/etcd-data",
			"--listen-client-urls", "http://0.0.0.0:2379",
			"--advertise-client-urls", fmt.Sprintf("http://%s:2379", n.Name),
			"--listen-peer-urls", "http://0.0.0.0:2380",
			"--initial-advertise-peer-urls", fmt.Sprintf("http://%s:2380", n.Name),
			"--initial-cluster", c.initialCluster(),
			"--initial-cluster-state", "new",
			"--initial-cluster-token", "lincheck",
			// Short election timings so a fault produces a leader change
			// inside a schedule rather than after it.
			"--heartbeat-interval", "100",
			"--election-timeout", "1000",
		}
		if _, err := dockerfault.Run(ctx, args...); err != nil {
			return err
		}
	}

	return dockerfault.WaitHealthy(ctx, 60*time.Second, func(ctx context.Context) error {
		cl := c.clientFor(0)
		hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := cl.Put(hctx, "__lincheck_health", []byte("ok")); err != nil {
			return err
		}
		return cl.Delete(hctx, "__lincheck_health")
	})
}

func (c *Cluster) Teardown(ctx context.Context) error {
	for _, n := range c.Nodes {
		dockerfault.MustRun(ctx, "rm", "-f", "-v", n.Name)
	}
	dockerfault.MustRun(ctx, "network", "rm", network)
	return nil
}

func (c *Cluster) clientFor(i int) *Client {
	n := c.Nodes[i%len(c.Nodes)]
	return &Client{
		base: fmt.Sprintf("http://127.0.0.1:%d", n.ClientPort),
		node: n.Name,
		http: c.http,
	}
}

func (c *Cluster) Clients(_ context.Context, n int) ([]lincheck.Client, error) {
	out := make([]lincheck.Client, n)
	for i := range out {
		out[i] = c.clientFor(i)
	}
	return out, nil
}

// Faults returns the fault set. Each node can be paused, killed, or cut off
// from the cluster network.
func (c *Cluster) Faults() []lincheck.Fault {
	var out []lincheck.Fault
	for _, n := range c.Nodes {
		out = append(out,
			dockerfault.Pause{Container: n.Name},
			dockerfault.Kill{Container: n.Name},
			dockerfault.Partition{Container: n.Name, Network: network, Alias: n.Name},
		)
	}
	return out
}

// FaultNames lists the faults, for the record in a report.
func (c *Cluster) FaultNames() []string {
	var out []string
	for _, f := range c.Faults() {
		out = append(out, f.Name())
	}
	return out
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client speaks the v3 gateway JSON API against one node.
type Client struct {
	base string
	node string
	http *http.Client
}

func (c *Client) Node() string { return c.node }
func (c *Client) Close() error { return nil }

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// A dial failure to a stopped or partitioned node means the request
		// never left this machine, so it definitively did not happen. Anything
		// else (a timeout, a broken connection mid-flight) is indeterminate.
		if isDialFailure(err) {
			return fmt.Errorf("%v: %w", err, lincheck.ErrRejected)
		}
		return err
	}
	defer resp.Body.Close()

	var raw json.RawMessage
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		// etcd's gateway reports "etcdserver: request timed out" and
		// "etcdserver: no leader" the same way. They are not the same thing:
		// "no leader" means the request was refused before entering the raft
		// log, "request timed out" means it may well be sitting in the log.
		// Getting this wrong in either direction corrupts the history.
		s := string(raw)
		if strings.Contains(s, "no leader") || strings.Contains(s, "not capable") {
			return fmt.Errorf("%s: %w", strings.TrimSpace(s), lincheck.ErrRejected)
		}
		return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(s))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func isDialFailure(err error) bool {
	var ne net.Error
	if _, ok := err.(net.Error); ok {
		_ = ne
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "No connection could be made") ||
		strings.Contains(s, "actively refused") ||
		strings.Contains(s, "dial tcp")
}

func b64(s []byte) string { return base64.StdEncoding.EncodeToString(s) }

type rangeResp struct {
	Kvs []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"kvs"`
}

// Get performs a LINEARIZABLE read. The gateway's default for Range is
// serializable=false, and this adapter never sets serializable, so the request
// goes through a quorum ReadIndex round trip. Setting it would be checking a
// weaker guarantee than the one etcd documents, and would produce "violations"
// that are documented behavior.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var out rangeResp
	err := c.post(ctx, "/v3/kv/range", map[string]any{"key": b64([]byte(key))}, &out)
	if err != nil {
		return nil, false, err
	}
	if len(out.Kvs) == 0 {
		return nil, false, nil
	}
	v, err := base64.StdEncoding.DecodeString(out.Kvs[0].Value)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.post(ctx, "/v3/kv/put", map[string]any{
		"key": b64([]byte(key)), "value": b64(value),
	}, nil)
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.post(ctx, "/v3/kv/deleterange", map[string]any{"key": b64([]byte(key))}, nil)
}
