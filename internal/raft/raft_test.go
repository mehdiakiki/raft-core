package raft_test

// Unit tests for the core Raft algorithm.
//
// These tests exercise RaftNode directly using a MockPeer that gives full
// control over which RPCs succeed or fail, making timing-independent
// assertions about algorithm correctness possible.
//
// See cluster_test.go for integration tests that run actual gRPC nodes
// connected via in-memory bufconn transports.

import (
	"context"
	"testing"
	"time"

	"github.com/medvih/raft-core/internal/raft"
	"github.com/medvih/raft-core/internal/storage"
)

// ── MockPeer ─────────────────────────────────────────────────────────────────

// MockPeer is a controllable Peer that records calls and returns preset replies.
type MockPeer struct {
	voteReply   raft.RequestVoteReply
	voteErr     error
	appendReply raft.AppendEntriesReply
	appendErr   error
	voteCalls   int
	appendCalls int
}

func (m *MockPeer) RequestVote(_ context.Context, _ raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	m.voteCalls++
	return m.voteReply, m.voteErr
}

func (m *MockPeer) AppendEntries(_ context.Context, _ raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	m.appendCalls++
	return m.appendReply, m.appendErr
}

func (m *MockPeer) PreVote(_ context.Context, args raft.PreVoteArgs) (raft.PreVoteReply, error) {
	return raft.PreVoteReply{VoteGranted: m.voteReply.VoteGranted, Term: args.NextTerm}, m.voteErr
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// makeNode creates a RaftNode with the given peers.
func makeNode(id string, peers map[string]raft.Peer) *raft.Node {
	return raft.New(raft.Config{ID: id, Peers: peers})
}

// collectState drains up to n state updates from StateCh within the deadline.
func collectState(n *raft.Node, max int, deadline time.Duration) []raft.StateSnapshot {
	var snaps []raft.StateSnapshot
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for len(snaps) < max {
		select {
		case s := <-n.StateCh:
			snaps = append(snaps, s)
		case <-timer.C:
			return snaps
		}
	}
	return snaps
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestInitialState(t *testing.T) {
	node := makeNode("A", nil)
	snap := node.Snapshot()

	if snap.State.String() != "FOLLOWER" {
		t.Errorf("expected FOLLOWER, got %s", snap.State)
	}
	if snap.CurrentTerm != 0 {
		t.Errorf("expected term 0, got %d", snap.CurrentTerm)
	}
	if snap.VotedFor != "" {
		t.Errorf("expected empty votedFor, got %q", snap.VotedFor)
	}
	if len(snap.Log) != 0 {
		t.Errorf("expected empty log, got %d entries", len(snap.Log))
	}
}

// §5.2: A follower grants a vote to the first valid candidate in a given term.
func TestRequestVote_GrantsVote_WhenFirstCandidateInTerm(t *testing.T) {
	node := makeNode("A", nil)

	reply := node.RequestVote(raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  "B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Error("expected vote to be granted")
	}
	if reply.Term != 1 {
		t.Errorf("expected reply term 1, got %d", reply.Term)
	}
}

// §5.2: A node may not vote twice in the same term.
func TestRequestVote_DeniesVote_WhenAlreadyVotedInSameTerm(t *testing.T) {
	node := makeNode("A", nil)

	// First vote for B.
	node.RequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "B"})

	// Second vote for C in the same term must be refused.
	reply := node.RequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "C"})
	if reply.VoteGranted {
		t.Error("expected vote to be denied for second candidate in same term")
	}
}

// §5.1: A node rejects RPCs from a stale term.
func TestRequestVote_DeniesVote_WhenRequestTermIsStale(t *testing.T) {
	node := makeNode("A", nil)
	// Advance node's term.
	node.RequestVote(raft.RequestVoteArgs{Term: 5, CandidateID: "B"})

	reply := node.RequestVote(raft.RequestVoteArgs{
		Term:        3,
		CandidateID: "C",
	})
	if reply.VoteGranted {
		t.Error("expected vote to be denied for stale term")
	}
}

// §5.1: A node updates its term and converts to follower on seeing a higher term.
func TestRequestVote_UpdatesTermAndStepsDown_WhenHigherTermReceived(t *testing.T) {
	node := makeNode("A", nil)

	node.RequestVote(raft.RequestVoteArgs{Term: 10, CandidateID: "B"})

	snap := node.Snapshot()
	if snap.CurrentTerm != 10 {
		t.Errorf("expected term 10, got %d", snap.CurrentTerm)
	}
	if snap.State.String() != "FOLLOWER" {
		t.Errorf("expected FOLLOWER after seeing higher term, got %s", snap.State)
	}
}

// §5.4.1: A node refuses to vote for a candidate whose log is less up-to-date.
func TestRequestVote_DeniesVote_WhenCandidateLogIsStale(t *testing.T) {
	node := makeNode("A", nil)
	// Give the node a log entry so it has a non-zero last log term.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []raft.LogEntry{{Term: 1, Command: "set x=1"}},
		LeaderCommit: 0,
	})

	// Candidate with empty log (lastLogTerm=0) should be refused.
	reply := node.RequestVote(raft.RequestVoteArgs{
		Term:         2,
		CandidateID:  "C",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply.VoteGranted {
		t.Error("expected vote to be denied for candidate with stale log")
	}
}

// §5.2: AppendEntries from a valid leader resets the election timer (follower
// stays follower and updates leaderID).
func TestAppendEntries_ResetsElectionTimer_WhenValidHeartbeatReceived(t *testing.T) {
	node := makeNode("A", nil)

	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "B",
	})

	if !reply.Success {
		t.Error("expected heartbeat to succeed")
	}
	snap := node.Snapshot()
	if snap.State.String() != "FOLLOWER" {
		t.Errorf("expected FOLLOWER, got %s", snap.State)
	}
	if snap.LeaderID != "B" {
		t.Errorf("expected leaderID B, got %q", snap.LeaderID)
	}
}

// §5.3 Rule 1: AppendEntries is rejected when the sender's term is stale.
func TestAppendEntries_RejectsRequest_WhenSenderTermIsStale(t *testing.T) {
	node := makeNode("A", nil)
	// Advance node to term 3.
	node.RequestVote(raft.RequestVoteArgs{Term: 3, CandidateID: "B"})

	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:     2,
		LeaderID: "C",
	})
	if reply.Success {
		t.Error("expected AppendEntries to be rejected for stale term")
	}
}

// §5.3 Rule 2: AppendEntries is rejected when prevLog doesn't match.
func TestAppendEntries_RejectsRequest_WhenPrevLogMismatch(t *testing.T) {
	node := makeNode("A", nil)
	// Append one entry at index 1 with term 1.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []raft.LogEntry{{Term: 1, Command: "x"}},
	})

	// Try to append at index 2 claiming prevLog term is 2 (actual is 1).
	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:         2,
		LeaderID:     "B",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // wrong
		Entries:      []raft.LogEntry{{Term: 2, Command: "y"}},
	})
	if reply.Success {
		t.Error("expected AppendEntries to fail on prevLog mismatch")
	}
}

// §5.3 Rule 3+4: Existing conflicting entries are deleted and new entries appended.
func TestAppendEntries_TruncatesConflictingEntries_WhenNewEntriesArrive(t *testing.T) {
	node := makeNode("A", nil)
	// Append two entries from an old leader (term 1).
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "B",
		Entries:  []raft.LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}},
	})

	// New leader (term 2) overwrites from index 2 onward.
	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:         2,
		LeaderID:     "C",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []raft.LogEntry{{Term: 2, Command: "X"}},
	})
	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	snap := node.Snapshot()
	if len(snap.Log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap.Log))
	}
	if snap.Log[1].Command != "X" {
		t.Errorf("expected second entry command X, got %q", snap.Log[1].Command)
	}
}

// §5.3 Rule 5: commitIndex advances when leaderCommit > commitIndex.
func TestAppendEntries_AdvancesCommitIndex_WhenLeaderCommitIsHigher(t *testing.T) {
	node := makeNode("A", nil)
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		Entries:      []raft.LogEntry{{Term: 1, Command: "x"}},
		LeaderCommit: 1,
	})

	snap := node.Snapshot()
	if snap.CommitIndex != 1 {
		t.Errorf("expected commitIndex 1, got %d", snap.CommitIndex)
	}
}

// SubmitCommand is rejected on a follower and returns the known leaderID.
func TestSubmitCommand_ReturnsLeaderHint_WhenNodeIsFollower(t *testing.T) {
	node := makeNode("A", nil)
	// Record leaderID by accepting a heartbeat.
	node.AppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "B"})

	result := node.SubmitCommand(raft.CommandRequest{Command: "set x=1"})
	if result.Accepted {
		t.Error("expected command to be rejected on follower")
	}
	if result.LeaderID != "B" {
		t.Errorf("expected redirect to leader B, got %q", result.LeaderID)
	}
}

// StateSnapshot is emitted every time observable state changes.
func TestStateCh_EmitsUpdate_WhenStateChanges(t *testing.T) {
	node := makeNode("A", nil)

	// Trigger a state change by accepting a heartbeat.
	node.AppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "B"})

	select {
	case snap := <-node.StateCh:
		if snap.NodeID != "A" {
			t.Errorf("expected node ID A, got %q", snap.NodeID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for state snapshot")
	}
}

// Kill transitions the node to Dead; Restart brings it back as Follower.
func TestKillAndRestart_TransitionsToDeadAndBack(t *testing.T) {
	node := makeNode("A", nil)
	node.Kill()

	if snap := node.Snapshot(); snap.State.String() != "DEAD" {
		t.Errorf("expected DEAD after kill, got %s", snap.State)
	}

	node.Restart()
	if snap := node.Snapshot(); snap.State.String() != "FOLLOWER" {
		t.Errorf("expected FOLLOWER after restart, got %s", snap.State)
	}
}

// A dead node ignores all RPCs.
func TestDeadNode_IgnoresAllRPCs(t *testing.T) {
	node := makeNode("A", nil)
	node.Kill()

	vr := node.RequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "B"})
	if vr.VoteGranted {
		t.Error("dead node must not grant votes")
	}

	ar := node.AppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "B"})
	if ar.Success {
		t.Error("dead node must not accept AppendEntries")
	}
}

// Election: a single node with peers that all grant votes must become leader
// within two election timeouts.
func TestElection_NodeBecomesLeader_WhenMajorityGrantsVotes(t *testing.T) {
	grantingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	peers := map[string]raft.Peer{
		"B": grantingPeer(),
		"C": grantingPeer(),
		"D": grantingPeer(),
		"E": grantingPeer(),
	}
	node := makeNode("A", peers)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap := node.Snapshot(); snap.State.String() == "LEADER" {
			return //
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("node did not become leader within 2 seconds")
}

// A candidate that cannot reach a majority must remain Candidate (no split-brain).
func TestElection_NodeStaysCandidate_WhenNoMajorityAvailable(t *testing.T) {
	refusingPeer := func() raft.Peer {
		return &MockPeer{
			voteReply: raft.RequestVoteReply{Term: 0, VoteGranted: false},
		}
	}
	peers := map[string]raft.Peer{
		"B": refusingPeer(),
		"C": refusingPeer(),
		"D": refusingPeer(),
		"E": refusingPeer(),
	}
	node := makeNode("A", peers)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	go node.Run(ctx)

	// Wait long enough for at least one election timeout to fire.
	time.Sleep(600 * time.Millisecond)

	snap := node.Snapshot()
	if snap.State.String() == "LEADER" {
		t.Error("node with no majority must not become leader")
	}
}

// §8: A new leader must append a no-op entry in its own term immediately on election.
// This ensures entries from previous terms can be committed indirectly.
func TestBecomeLeader_AppendsNoopEntry_OnElection(t *testing.T) {
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
	node := makeNode("A", peers)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	// Wait until leader.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := node.Snapshot(); s.State.String() == "LEADER" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	snap := node.Snapshot()
	if snap.State.String() != "LEADER" {
		t.Fatal("node did not become leader")
	}

	// The first log entry must be a no-op.
	if len(snap.Log) == 0 {
		t.Fatal("expected at least one log entry (noop) after becoming leader")
	}
	if snap.Log[0].Type != raft.LogEntryNoop {
		t.Errorf("expected first log entry to be LogEntryNoop, got %v", snap.Log[0].Type)
	}
	if snap.Log[0].Command != "" {
		t.Errorf("noop entry should have empty command, got %q", snap.Log[0].Command)
	}
}

// §5.1: stepDownToFollower must persist the cleared votedFor so a crashed node
// restarting in the new term cannot double-vote.
func TestStepDown_PersistsVotedFor_AfterStepDownDueToHigherTerm(t *testing.T) {
	mem := storage.NewMemoryStore()
	node := raft.New(raft.Config{ID: "A", Peers: nil, Storage: mem})

	// Grant a vote to B in term 1 (votedFor="B" is now persisted).
	node.RequestVote(raft.RequestVoteArgs{
		Term:        1,
		CandidateID: "B",
	})

	// Receive an AppendEntries from term 2, forcing a step-down.
	// stepDownToFollower sets votedFor="" and should persist it.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:     2,
		LeaderID: "C",
	})

	// Simulate a crash: the in-memory state is gone, load from storage.
	node2 := raft.New(raft.Config{ID: "A", Peers: nil, Storage: mem})
	snap := node2.Snapshot()

	if snap.CurrentTerm != 2 {
		t.Errorf("expected term 2 after step-down, got %d", snap.CurrentTerm)
	}
	if snap.VotedFor != "" {
		t.Errorf("expected votedFor to be cleared after step-down, got %q", snap.VotedFor)
	}
}

// NoopEntry type must survive a storage round-trip.
// Without this, a restarted node would treat the leader's no-op as a command.
func TestLogEntryType_NoopSurvivesStorageRoundtrip(t *testing.T) {
	mem := storage.NewMemoryStore()
	entries := []raft.LogEntry{
		{Term: 1, Type: raft.LogEntryNoop, Command: ""},
		{Term: 1, Type: raft.LogEntryCommand, Command: "set x=1"},
	}
	if err := mem.StoreLog(entries); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

	got, err := mem.GetLog(1, 3)
	if err != nil {
		t.Fatalf("GetLog: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Type != raft.LogEntryNoop {
		t.Errorf("entry[0]: expected LogEntryNoop, got %v", got[0].Type)
	}
	if got[1].Type != raft.LogEntryCommand {
		t.Errorf("entry[1]: expected LogEntryCommand, got %v", got[1].Type)
	}
}

// Config.Storage=nil must still provide real in-memory durability semantics.
func TestNew_WithNilStorage_UsesRealMemoryStore(t *testing.T) {
	node := raft.New(raft.Config{ID: "A"})

	node.RequestVote(raft.RequestVoteArgs{
		Term:        3,
		CandidateID: "B",
	})
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         3,
		LeaderID:     "L",
		Entries:      []raft.LogEntry{{Term: 3, Type: raft.LogEntryCommand, Command: "cmd-1"}},
		LeaderCommit: 1,
	})

	node.Kill()
	node.Restart()

	snap := node.Snapshot()
	if snap.State.String() != "FOLLOWER" {
		t.Fatalf("expected FOLLOWER after restart, got %s", snap.State)
	}
	if snap.CurrentTerm != 3 {
		t.Fatalf("expected term 3 after restart, got %d", snap.CurrentTerm)
	}
	if snap.VotedFor != "B" {
		t.Fatalf("expected votedFor B after restart, got %q", snap.VotedFor)
	}
	if snap.CommitIndex != 1 {
		t.Fatalf("expected commitIndex 1 after restart, got %d", snap.CommitIndex)
	}
	if len(snap.Log) != 1 {
		t.Fatalf("expected one persisted log entry, got %d", len(snap.Log))
	}
	if snap.Log[0].Command != "cmd-1" {
		t.Fatalf("expected persisted command cmd-1, got %q", snap.Log[0].Command)
	}
}
