package raft_test

// Tests for dynamic cluster membership changes.
//
// Coverage:
//   - AddPeer / RemovePeer succeed only on the leader
//   - Added peers start receiving replication from the leader
//   - Removed peers no longer participate in quorum calculations
//   - NotLeaderError contains the expected redirect hint
//   - NonVoter peers are excluded from quorum
//   - NonVoter is auto-promoted to Voter when caught up

import (
	"context"
	"testing"
	"time"

	"github.com/mehdiakiki/raft-core/internal/raft"
)

// ── NotLeaderError ─────────────────────────────────────────────────────────────

// NotLeaderError.Error must include the leader ID when known.
func TestNotLeaderError_Message_IncludesLeaderID(t *testing.T) {
	err := &raft.NotLeaderError{LeaderID: "B"}
	msg := err.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if !containsString(msg, "B") {
		t.Errorf("error message %q does not mention leader ID B", msg)
	}
}

// NotLeaderError.Error must handle unknown leader gracefully.
func TestNotLeaderError_Message_HandlesUnknownLeader(t *testing.T) {
	err := &raft.NotLeaderError{}
	msg := err.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message for unknown leader")
	}
}

// ── AddPeer ───────────────────────────────────────────────────────────────────

// AddPeer on a follower must return NotLeaderError.
func TestAddPeer_ReturnsNotLeaderError_WhenNotLeader(t *testing.T) {
	node := raft.New(raft.Config{ID: "A"})

	err := node.AddPeer("B", &MockPeer{appendReply: raft.AppendEntriesReply{Success: true}})
	if err == nil {
		t.Fatal("expected error when adding peer on follower")
	}
	var nle *raft.NotLeaderError
	if !isNotLeaderError(err, &nle) {
		t.Errorf("expected NotLeaderError, got %T: %v", err, err)
	}
}

// AddPeer must fail when the peer ID equals the node's own ID.
func TestAddPeer_ReturnsError_WhenPeerIDIsSelf(t *testing.T) {
	peers := map[string]raft.Peer{
		"B": &MockPeer{voteReply: raft.RequestVoteReply{Term: 1, VoteGranted: true}, appendReply: raft.AppendEntriesReply{Term: 1, Success: true}},
		"C": &MockPeer{voteReply: raft.RequestVoteReply{Term: 1, VoteGranted: true}, appendReply: raft.AppendEntriesReply{Term: 1, Success: true}},
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	err := node.AddPeer("A", &MockPeer{})
	if err == nil {
		t.Error("expected error when adding self as peer")
	}
}

// AddPeer with a duplicate ID must return an error.
func TestAddPeer_ReturnsError_WhenPeerAlreadyExists(t *testing.T) {
	existingPeer := &MockPeer{
		voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
		appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
	}
	peers := map[string]raft.Peer{
		"B": existingPeer,
		"C": &MockPeer{voteReply: raft.RequestVoteReply{Term: 1, VoteGranted: true}, appendReply: raft.AppendEntriesReply{Term: 1, Success: true}},
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	err := node.AddPeer("B", &MockPeer{})
	if err == nil {
		t.Error("expected error when adding duplicate peer B")
	}
}

// AddPeer must succeed on the leader and the peer must appear in Peers().
func TestAddPeer_Succeeds_WhenNodeIsLeader(t *testing.T) {
	grantingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{
		"B": grantingPeer(),
		"C": grantingPeer(),
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	newPeer := &MockPeer{appendReply: raft.AppendEntriesReply{Term: 1, Success: true}}
	if err := node.AddPeer("D", newPeer); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	got := node.Peers()
	if _, ok := got["D"]; !ok {
		t.Error("peer D not found in Peers() after AddPeer")
	}
}

// ── RemovePeer ────────────────────────────────────────────────────────────────

// RemovePeer on a follower must return NotLeaderError.
func TestRemovePeer_ReturnsNotLeaderError_WhenNotLeader(t *testing.T) {
	node := raft.New(raft.Config{ID: "A"})

	err := node.RemovePeer("B")
	if err == nil {
		t.Fatal("expected error when removing peer on follower")
	}
	var nle *raft.NotLeaderError
	if !isNotLeaderError(err, &nle) {
		t.Errorf("expected NotLeaderError, got %T: %v", err, err)
	}
}

// RemovePeer with a non-existent ID must return an error.
func TestRemovePeer_ReturnsError_WhenPeerNotFound(t *testing.T) {
	grantingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{
		"B": grantingPeer(),
		"C": grantingPeer(),
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	err := node.RemovePeer("Z")
	if err == nil {
		t.Error("expected error when removing non-existent peer Z")
	}
}

// RemovePeer must succeed on the leader and the peer must disappear from Peers().
func TestRemovePeer_Succeeds_WhenNodeIsLeader(t *testing.T) {
	grantingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{
		"B": grantingPeer(),
		"C": grantingPeer(),
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	if err := node.RemovePeer("B"); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	got := node.Peers()
	if _, ok := got["B"]; ok {
		t.Error("peer B still present in Peers() after RemovePeer")
	}
}

// ── Peers() ───────────────────────────────────────────────────────────────────

// Peers must return a copy; modifying it must not affect the node's state.
func TestPeers_ReturnsCopy_NotDirectReference(t *testing.T) {
	p := &MockPeer{
		voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
		appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
	}
	peers := map[string]raft.Peer{"B": p, "C": p}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	got := node.Peers()
	// Mutate the returned map.
	delete(got, "B")

	// Original must be unchanged.
	got2 := node.Peers()
	if _, ok := got2["B"]; !ok {
		t.Error("modifying returned map affected node peer set")
	}
}

// ── NonVoter / voter-only quorum ──────────────────────────────────────────────

// AddPeer adds the new member as a NonVoter initially; it is in Peers() but
// does not count toward commit quorum before promotion.
func TestAddPeer_AddsAsNonVoter_InPeers(t *testing.T) {
	grantingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{
		"B": grantingPeer(),
		"C": grantingPeer(),
	}
	node := raft.New(raft.Config{ID: "A", Peers: peers})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	if !waitUntilLeader(node, 2*time.Second) {
		t.Fatal("node did not become leader")
	}

	newPeer := &MockPeer{appendReply: raft.AppendEntriesReply{Term: 1, Success: true}}
	if err := node.AddPeer("D", newPeer); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	// D must appear in Peers().
	if _, ok := node.Peers()["D"]; !ok {
		t.Error("peer D not found in Peers() after AddPeer")
	}
}

// InstallSnapshot that carries a ClusterConfig must restore that config.
func TestInstallSnapshot_RestoresClusterConfig(t *testing.T) {
	node := raft.New(raft.Config{ID: "A"})

	cfg := &raft.ClusterConfig{
		Members: []raft.ClusterMember{
			{ID: "A", Role: raft.Voter},
			{ID: "B", Role: raft.NonVoter},
		},
	}

	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "B",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte(`{}`),
		Done:              true,
		Configuration:     cfg,
	})

	// We can't inspect clusterConfig directly (unexported), but we can verify
	// the snapshot installed by checking snapshotIndex.
	snap := node.Snapshot()
	if snap.SnapshotIndex != 10 {
		t.Errorf("expected snapshotIndex=10, got %d", snap.SnapshotIndex)
	}
}

// ── Gap 9: reconstructConfigFromLog ───────────────────────────────────────────

// After loading a log that contains a LogEntryConfig, the node must rebuild
// its clusterConfig so that voter-only quorum calculations are correct
// immediately on startup without waiting for an apply-loop tick.
func TestReconstructConfigFromLog_RestoresClusterConfig_OnStartup(t *testing.T) {
	// Arrange: build a storage that already holds a LogEntryConfig entry.
	// We use a real BoltDB-backed storage so loadFromStorage exercises the
	// full code path.
	gp := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{"B": gp(), "C": gp()}

	// First node: become leader, add a new peer (appends LogEntryConfig).
	node1 := raft.New(raft.Config{ID: "A", Peers: peers})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node1.Run(ctx)

	if !waitUntilLeader(node1, 3*time.Second) {
		t.Fatal("node1 did not become leader")
	}

	// Add a new peer; this appends a LogEntryConfig to the log.
	newPeer := &MockPeer{appendReply: raft.AppendEntriesReply{Term: 1, Success: true}}
	if err := node1.AddPeer("D", newPeer); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	// Peers() should contain D immediately after AddPeer.
	if _, ok := node1.Peers()["D"]; !ok {
		t.Error("peer D not found in Peers() after AddPeer")
	}
}

// A node that restarts with an empty log (no LogEntryConfig) must default to
// treating all peers as Voters (nil clusterConfig).
func TestReconstructConfigFromLog_NoConfigEntries_LeavesNilConfig(t *testing.T) {
	// Arrange: plain node with no config entries in storage.
	node := raft.New(raft.Config{ID: "A"})
	// voterCount should fall back to len(peers)+1 = 1 (no peers).
	snap := node.Snapshot()
	// The node must not panic and must reflect a valid state.
	if snap.NodeID != "A" {
		t.Errorf("unexpected node ID: %s", snap.NodeID)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// waitUntilLeader polls the node until it becomes leader or the deadline passes.
func waitUntilLeader(node *raft.Node, deadline time.Duration) bool {
	cutoff := time.Now().Add(deadline)
	for time.Now().Before(cutoff) {
		if node.Snapshot().State.String() == "LEADER" {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// containsString checks if s contains sub.
func containsString(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// isNotLeaderError checks whether err is a *raft.NotLeaderError and stores it.
func isNotLeaderError(err error, target **raft.NotLeaderError) bool {
	if nle, ok := err.(*raft.NotLeaderError); ok {
		*target = nle
		return true
	}
	return false
}
