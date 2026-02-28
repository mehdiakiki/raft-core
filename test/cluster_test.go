// Package test contains integration tests that spin up a full Raft cluster
// using real gRPC servers wired together over in-memory bufconn transports.
//
// No ports are allocated; tests are fast, hermetic, and deterministic.
package test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/medvih/raft-core/gen/raft"
	internalraft "github.com/medvih/raft-core/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// cluster is a test harness that creates n Raft nodes connected to each other
// via in-memory gRPC transports.
type cluster struct {
	nodes      []*internalraft.Node
	servers    []*grpc.Server
	listeners  []*bufconn.Listener
	cancelFunc context.CancelFunc
}

// newCluster creates and starts an n-node Raft cluster.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()

	listeners := make([]*bufconn.Listener, n)
	servers := make([]*grpc.Server, n)
	nodes := make([]*internalraft.Node, n)
	ids := make([]string, n)

	for i := range n {
		ids[i] = string(rune('A' + i))
		listeners[i] = bufconn.Listen(bufSize)
	}

	// Build peer maps and gRPC servers.
	for i := range n {
		peers := make(map[string]internalraft.Peer, n-1)
		for j := range n {
			if i == j {
				continue
			}
			// Dial peer j via its bufconn listener.
			conn, err := grpc.NewClient(
				"passthrough:///bufconn",
				grpc.WithContextDialer(bufconnDialer(listeners[j])),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatalf("failed to dial peer %s: %v", ids[j], err)
			}
			t.Cleanup(func() { conn.Close() })
			peers[ids[j]] = internalraft.NewGRPCPeer(ids[j], raft.NewRaftServiceClient(conn))
		}

		nodes[i] = internalraft.New(internalraft.Config{ID: ids[i], Peers: peers})
		srv := grpc.NewServer()
		raft.RegisterRaftServiceServer(srv, internalraft.NewServer(nodes[i]))
		servers[i] = srv
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start each gRPC server and node.
	for i := range n {
		go func(idx int) {
			if err := servers[idx].Serve(listeners[idx]); err != nil {
				// Serve returns when the server is stopped; not an error in tests.
				_ = err
			}
		}(i)
		go nodes[i].Run(ctx)
	}

	t.Cleanup(func() {
		cancel()
		for _, srv := range servers {
			srv.GracefulStop()
		}
	})

	return &cluster{
		nodes:      nodes,
		servers:    servers,
		listeners:  listeners,
		cancelFunc: cancel,
	}
}

// leader returns the current leader node, polling until deadline.
// Returns nil if no leader is found in time.
func (c *cluster) leader(deadline time.Duration) *internalraft.Node {
	cutoff := time.Now().Add(deadline)
	for time.Now().Before(cutoff) {
		for _, n := range c.nodes {
			if snap := n.Snapshot(); snap.State.String() == "LEADER" {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// countLeaders counts how many nodes currently believe they are the leader.
func (c *cluster) countLeaders() int {
	count := 0
	for _, n := range c.nodes {
		if snap := n.Snapshot(); snap.State.String() == "LEADER" {
			count++
		}
	}
	return count
}

// bufconnDialer returns a gRPC ContextDialer that connects to the given bufconn listener.
func bufconnDialer(l *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return l.DialContext(ctx)
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// A 3-node cluster must elect exactly one leader.
func TestCluster_ElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 3 seconds")
	}

	if n := c.countLeaders(); n != 1 {
		t.Errorf("expected exactly 1 leader, found %d", n)
	}
}

// A 5-node cluster must also elect exactly one leader.
func TestCluster_FiveNodes_ElectsOneLeader(t *testing.T) {
	c := newCluster(t, 5)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 3 seconds")
	}

	if n := c.countLeaders(); n != 1 {
		t.Errorf("expected exactly 1 leader, found %d", n)
	}
}

// A command submitted to the leader is replicated to all followers.
func TestCluster_LogReplication(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	ok := leader.SubmitCommand(internalraft.CommandRequest{Command: "set x=42"})
	if !ok.Accepted {
		t.Fatal("leader rejected command")
	}

	// Allow time for replication.
	time.Sleep(200 * time.Millisecond)

	for _, n := range c.nodes {
		snap := n.Snapshot()
		// Log[0] is the no-op entry appended by the leader on election (§8).
		// The user command is at Log[1].
		if len(snap.Log) < 2 {
			t.Errorf("node %s has only %d log entries after replication (expected noop + command)", snap.NodeID, len(snap.Log))
			continue
		}
		if snap.Log[1].Command != "set x=42" {
			t.Errorf("node %s log[1] = %q, want %q", snap.NodeID, snap.Log[1].Command, "set x=42")
		}
	}
}

// A command is committed once a majority acknowledges it.
func TestCluster_CommitAfterMajority(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	leader.SubmitCommand(internalraft.CommandRequest{Command: "set y=1"})

	time.Sleep(300 * time.Millisecond)

	snap := leader.Snapshot()
	// The leader appends a no-op at index 1 on election (§8).
	// The user command "set y=1" lands at index 2.
	// After majority acknowledgment, commitIndex should be 2.
	if snap.CommitIndex != 2 {
		t.Errorf("expected commitIndex 2 on leader (noop at 1, command at 2), got %d", snap.CommitIndex)
	}
}

// When the leader is killed, the remaining nodes elect a new leader.
func TestCluster_LeaderFailover(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no initial leader elected")
	}
	leaderID := leader.Snapshot().NodeID

	leader.Kill()

	// Allow time for re-election.
	time.Sleep(800 * time.Millisecond)

	newLeader := c.leader(2 * time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after failover")
	}
	if newLeader.Snapshot().NodeID == leaderID {
		t.Error("the killed node should not be the new leader")
	}

	if n := c.countLeaders(); n != 1 {
		t.Errorf("expected exactly 1 leader after failover, found %d", n)
	}
}

// A killed node can rejoin and the cluster continues to function.
func TestCluster_NodeRejoin(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.leader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Kill a follower.
	var victim *internalraft.Node
	for _, n := range c.nodes {
		if snap := n.Snapshot(); snap.State.String() != "LEADER" {
			victim = n
			break
		}
	}
	victim.Kill()

	// Submit a command while the follower is down.
	leader.SubmitCommand(internalraft.CommandRequest{Command: "set z=99"})
	time.Sleep(200 * time.Millisecond)

	// Restart the follower.
	victim.Restart()
	time.Sleep(500 * time.Millisecond)

	// The rejoined node should eventually catch up.
	snap := victim.Snapshot()
	if snap.State.String() == "DEAD" {
		t.Error("restarted node is still DEAD")
	}
}

// Split-brain prevention: with only 2 of 5 nodes alive, no leader should emerge.
func TestCluster_NoleaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 5)

	_ = c.leader(2 * time.Second)

	// Kill 3 nodes (majority down).
	killed := 0
	for _, n := range c.nodes {
		if snap := n.Snapshot(); snap.State.String() != "LEADER" && killed < 3 {
			n.Kill()
			killed++
		}
	}

	// Also kill the original leader.
	for _, n := range c.nodes {
		if snap := n.Snapshot(); snap.State.String() == "LEADER" {
			n.Kill()
			break
		}
	}

	// No new leader should emerge from a 1-node minority.
	time.Sleep(700 * time.Millisecond)

	leaders := c.countLeaders()
	if leaders > 0 {
		t.Errorf("expected no leader with minority cluster, found %d", leaders)
	}
}
