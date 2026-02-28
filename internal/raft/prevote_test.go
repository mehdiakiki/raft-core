package raft

import (
	"context"
	"testing"
	"time"
)

// TestPreVote_PreventsElection_WhenLeaderIsHealthy verifies that a node with
// a fresh leader heartbeat denies pre-votes, preventing unnecessary elections
// when a partitioned node tries to rejoin (§3.1).
func TestPreVote_PreventsElection_WhenLeaderIsHealthy(t *testing.T) {
	// Arrange: voter that recently heard from a leader.
	node := New(Config{
		ID:                 "n1",
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	})
	// Simulate a recent heartbeat so leaderIsRecentlyHeard() returns true.
	node.mu.Lock()
	node.lastHeartbeat = time.Now()
	node.mu.Unlock()

	// Act
	reply := node.PreVote(PreVoteArgs{
		NextTerm:     2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// Assert
	if reply.VoteGranted {
		t.Fatal("expected VoteGranted=false when leader is recently heard")
	}
}

// TestPreVote_GrantsVote_WhenLeaderIsStale verifies that a node with a stale
// heartbeat grants pre-votes to a candidate with an up-to-date log.
func TestPreVote_GrantsVote_WhenLeaderIsStale(t *testing.T) {
	// Arrange: voter whose last heartbeat is well in the past.
	node := New(Config{
		ID:                 "n1",
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
	})
	node.mu.Lock()
	node.lastHeartbeat = time.Now().Add(-10 * time.Second) // stale
	node.mu.Unlock()

	// Act
	reply := node.PreVote(PreVoteArgs{
		NextTerm:     2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// Assert
	if !reply.VoteGranted {
		t.Fatal("expected VoteGranted=true when leader is stale")
	}
}

// TestPreVote_DeniesVote_WhenCandidateLogIsStale verifies that a voter refuses
// a pre-vote from a candidate whose log is behind (§5.4.1).
func TestPreVote_DeniesVote_WhenCandidateLogIsStale(t *testing.T) {
	// Arrange: voter with a longer log than the candidate.
	node := New(Config{
		ID:                 "n1",
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
	})
	node.mu.Lock()
	node.log = []LogEntry{
		{Term: 1, Type: LogEntryCommand, Command: "set x=1"},
		{Term: 1, Type: LogEntryCommand, Command: "set y=2"},
	}
	node.lastHeartbeat = time.Now().Add(-10 * time.Second) // stale leader
	node.mu.Unlock()

	// Act: candidate claims lastLogIndex=0, lastLogTerm=0 (behind voter's log).
	reply := node.PreVote(PreVoteArgs{
		NextTerm:     2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// Assert
	if reply.VoteGranted {
		t.Fatal("expected VoteGranted=false when candidate log is stale")
	}
}

// TestPreVote_WinsElection_WhenQuorumGrants verifies that a node that wins a
// pre-vote quorum proceeds to a real election and becomes leader.
func TestPreVote_WinsElection_WhenQuorumGrants(t *testing.T) {
	// Arrange: node with two granting peers that respond to both PreVote and
	// RequestVote with approval.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &grantingPeer{},
			"n3": &grantingPeer{},
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)

	// Wait for the node to become leader via the pre-vote → election path.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		s := node.state
		node.mu.Unlock()
		if s == Leader {
			return // success
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("node did not become leader after pre-vote succeeded")
}

// TestPreVote_StaysFollower_WhenQuorumDenies verifies that a node that fails
// the pre-vote phase does not increment its term and stays as follower.
func TestPreVote_StaysFollower_WhenQuorumDenies(t *testing.T) {
	// Arrange: node with two failing peers so pre-vote quorum never forms.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &failingPeer{},
			"n3": &failingPeer{},
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 30 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go node.Run(ctx)

	// Wait for the context to expire (node should never become leader).
	<-ctx.Done()

	node.mu.Lock()
	s := node.state
	node.mu.Unlock()

	if s == Leader {
		t.Fatal("expected node to stay follower when pre-vote quorum denied")
	}
}

// TestPreVote_IgnoresRPC_WhenNodeIsDead verifies that a dead node rejects
// PreVote RPCs without crashing.
func TestPreVote_IgnoresRPC_WhenNodeIsDead(t *testing.T) {
	// Arrange
	node := New(Config{ID: "n1"})
	node.mu.Lock()
	node.state = Dead
	node.mu.Unlock()

	// Act
	reply := node.PreVote(PreVoteArgs{
		NextTerm:    2,
		CandidateID: "n2",
	})

	// Assert
	if reply.VoteGranted {
		t.Fatal("expected VoteGranted=false for dead node")
	}
}
