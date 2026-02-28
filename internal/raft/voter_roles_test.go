package raft

import (
	"context"
	"testing"
	"time"
)

func TestRequestVote_DeniesVote_WhenCandidateIsNonVoter(t *testing.T) {
	node := New(Config{ID: "n1"})
	node.mu.Lock()
	node.clusterConfig = &ClusterConfig{
		Members: []ClusterMember{
			{ID: "n1", Role: Voter},
			{ID: "n2", Role: NonVoter},
		},
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote denial for NonVoter candidate")
	}
}

func TestRequestVote_DeniesVote_WhenReceiverIsNonVoter(t *testing.T) {
	node := New(Config{ID: "n1"})
	node.mu.Lock()
	node.clusterConfig = &ClusterConfig{
		Members: []ClusterMember{
			{ID: "n1", Role: NonVoter},
			{ID: "n2", Role: Voter},
		},
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected non-voter receiver to deny RequestVote")
	}
}

func TestPreVote_DeniesVote_WhenCandidateIsNonVoter(t *testing.T) {
	node := New(Config{
		ID:                 "n1",
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
	})
	node.mu.Lock()
	node.clusterConfig = &ClusterConfig{
		Members: []ClusterMember{
			{ID: "n1", Role: Voter},
			{ID: "n2", Role: NonVoter},
		},
	}
	// Stale leader heartbeat to avoid denial for "leader fresh" reason.
	node.lastHeartbeat = time.Now().Add(-10 * time.Second)
	node.mu.Unlock()

	reply := node.PreVote(PreVoteArgs{
		NextTerm:     2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected pre-vote denial for NonVoter candidate")
	}
}

func TestSendPreVote_UsesVoterQuorumOnly(t *testing.T) {
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &failingPeer{},  // voter; denies quorum
			"n3": &grantingPeer{}, // non-voter; must be ignored
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 30 * time.Millisecond,
	})
	node.mu.Lock()
	node.clusterConfig = &ClusterConfig{
		Members: []ClusterMember{
			{ID: "n1", Role: Voter},
			{ID: "n2", Role: Voter},
			{ID: "n3", Role: NonVoter},
		},
	}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	node.sendPreVote(ctx)

	// Allow async pre-vote goroutines to finish.
	time.Sleep(50 * time.Millisecond)

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.state == Leader {
		t.Fatal("node became leader using NonVoter pre-vote; expected follower")
	}
	if node.currentTerm != 0 {
		t.Fatalf("expected term to remain 0 when voter quorum denied, got %d", node.currentTerm)
	}
}
