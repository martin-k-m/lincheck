// Package consul is a lincheck adapter for a real 3-server Consul cluster
// running in Docker containers.
//
// It uses Consul's HTTP KV API directly, with ?consistent=true on every read.
// That flag matters: Consul's default read mode is "default", which is served
// by the leader without a round trip and can be stale after a partition.
// Checking the default mode against linearizability would flag documented
// behavior, which this repository refuses to do.
package consul

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/martin-k-m/lincheck"
	"github.com/martin-k-m/lincheck/dockerfault"
)

// Image is the Consul image under test.
const Image = "hashicorp/consul:1.20"

const (
	network = "lincheck-consul"
	prefix  = "lincheck-consul-"
)

type Node struct {
	Name string
	Port int
}

// Cluster is a 3-server Consul cluster as a lincheck.Target.
type Cluster struct {
	Nodes []Node
	http  *http.Client
}

func New() *Cluster {
	return &Cluster{
		Nodes: []Node{
			{Name: prefix + "0", Port: 28501},
			{Name: prefix + "1", Port: 28502},
			{Name: prefix + "2", Port: 28503},
		},
		http: &http.Client{Transport: &http.Transport{
			DisableKeepAlives:   true,
			DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			TLSHandshakeTimeout: 2 * time.Second,
		}},
	}
}

func (c *Cluster) Name() string { return "Consul 1.20 (3-server cluster, Docker)" }

func (c *Cluster) DocumentedModel() string {
	return "Consul documents three read consistency modes for its KV store. \"consistent\" is " +
		"described as: \"This mode is strongly consistent without caveats. It requires that a " +
		"leader verify with a quorum of peers that it is still leader. This introduces an " +
		"additional round-trip to all server nodes.\" The \"default\" mode is documented as " +
		"possibly stale: \"in a very rare failure scenario ... the old leader may service some " +
		"reads\" (Consul docs, api-docs/features/consistency). Writes go through Raft. This " +
		"harness sets consistent=true on every read, so it is checking the mode Consul " +
		"documents as strongly consistent, not the stale-tolerant default."
}

func (c *Cluster) Setup(ctx context.Context) error {
	_ = c.Teardown(ctx)
	if _, err := dockerfault.Run(ctx, "network", "create", network); err != nil {
		return err
	}
	for i, n := range c.Nodes {
		args := []string{
			"run", "-d", "--name", n.Name, "--network", network, "--network-alias", n.Name,
			"-p", fmt.Sprintf("127.0.0.1:%d:8500", n.Port),
			Image, "agent", "-server",
			"-node=" + n.Name,
			"-bootstrap-expect=3",
			"-client=0.0.0.0",
			"-bind=0.0.0.0",
			"-data-dir=/consul/data",
			// Consul's raft timings are conservative by default; shorten them
			// so a fault inside a schedule actually causes a leader change.
			`-hcl=performance { raft_multiplier = 1 }`,
		}
		if i > 0 {
			args = append(args, "-retry-join="+c.Nodes[0].Name)
		} else {
			args = append(args, "-retry-join="+c.Nodes[1].Name, "-retry-join="+c.Nodes[2].Name)
		}
		if _, err := dockerfault.Run(ctx, args...); err != nil {
			return err
		}
	}
	return dockerfault.WaitHealthy(ctx, 90*time.Second, func(ctx context.Context) error {
		cl := c.clientFor(0)
		hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := cl.Put(hctx, "__lincheck_health", []byte("ok")); err != nil {
			return err
		}
		if _, found, err := cl.Get(hctx, "__lincheck_health"); err != nil || !found {
			return fmt.Errorf("health read: found=%v err=%v", found, err)
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
	return &Client{base: fmt.Sprintf("http://127.0.0.1:%d", n.Port), node: n.Name, http: c.http}
}

func (c *Cluster) Clients(_ context.Context, n int) ([]lincheck.Client, error) {
	out := make([]lincheck.Client, n)
	for i := range out {
		out[i] = c.clientFor(i)
	}
	return out, nil
}

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

// ---------------------------------------------------------------------------

type Client struct {
	base string
	node string
	http *http.Client
}

func (c *Client) Node() string { return c.node }
func (c *Client) Close() error { return nil }

func (c *Client) do(ctx context.Context, method, path string, body io.Reader) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if isDialFailure(err) {
			return nil, 0, fmt.Errorf("%v: %w", err, lincheck.ErrRejected)
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.StatusCode, err
}

func isDialFailure(err error) bool {
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "actively refused") ||
		strings.Contains(s, "No connection could be made")
}

// rejected classifies a Consul error body. "No cluster leader" is Consul
// telling us it refused the request outright, so the operation definitely did
// not happen. Anything else is left indeterminate.
func rejected(status int, body []byte) error {
	s := strings.TrimSpace(string(body))
	if strings.Contains(s, "No cluster leader") || strings.Contains(s, "no cluster leader") {
		return fmt.Errorf("consul: %s: %w", s, lincheck.ErrRejected)
	}
	return fmt.Errorf("consul: HTTP %d: %s", status, s)
}

type kvEntry struct {
	Value string `json:"Value"`
}

// Get reads with consistent=true, the mode Consul documents as strongly
// consistent.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, status, err := c.do(ctx, http.MethodGet, "/v1/kv/"+key+"?consistent=true", nil)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, rejected(status, b)
	}
	var entries []kvEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, false, fmt.Errorf("consul: decode: %w", err)
	}
	if len(entries) == 0 {
		return nil, false, nil
	}
	v, err := base64.StdEncoding.DecodeString(entries[0].Value)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	b, status, err := c.do(ctx, http.MethodPut, "/v1/kv/"+key, strings.NewReader(string(value)))
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return rejected(status, b)
	}
	if strings.TrimSpace(string(b)) != "true" {
		// Consul returns the literal "false" when the write was not applied.
		// That is a definite negative, not an unknown.
		return fmt.Errorf("consul: put returned %q: %w", strings.TrimSpace(string(b)), lincheck.ErrRejected)
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	b, status, err := c.do(ctx, http.MethodDelete, "/v1/kv/"+key, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return rejected(status, b)
	}
	return nil
}
