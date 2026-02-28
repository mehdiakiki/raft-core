package raft_test

// Tests for client session deduplication (§8 "Client Interaction").
//
// The Raft paper requires that each client request carries a unique serial
// number so the leader can return a cached result instead of re-applying a
// command that was already committed.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/medvih/raft-core/internal/raft"
	"github.com/medvih/raft-core/internal/storage"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// waitForLeader polls until node reaches Leader state or the deadline passes.
func waitForLeader(t *testing.T, node *raft.Node, deadline time.Duration) {
	t.Helper()
	cutoff := time.Now().Add(deadline)
	for time.Now().Before(cutoff) {
		if node.Snapshot().State == raft.Leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node did not become leader within deadline")
}

// waitForCommitIndex waits until commitIndex >= target or the deadline passes.
func waitForCommitIndex(t *testing.T, node *raft.Node, target int64, deadline time.Duration) {
	t.Helper()
	cutoff := time.Now().Add(deadline)
	for time.Now().Before(cutoff) {
		if node.Snapshot().CommitIndex >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node did not reach commitIndex >= %d within deadline", target)
}

// grantingPeers returns a set of MockPeer instances that always grant votes and
// always acknowledge AppendEntries.
func grantingPeers(ids ...string) map[string]raft.Peer {
	peers := make(map[string]raft.Peer, len(ids))
	for _, id := range ids {
		peers[id] = &MockPeer{
			voteReply:   raft.RequestVoteReply{Term: 1, VoteGranted: true},
			appendReply: raft.AppendEntriesReply{Term: 1, Success: true},
		}
	}
	return peers
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

// §8: A command without client session fields is accepted without deduplication.
func TestSubmitCommand_AcceptsCommand_WhenNoClientSession(t *testing.T) {
	// ARRANGE
	node := raft.New(raft.Config{ID: "A", Peers: grantingPeers("B", "C")})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	// ACT
	result := node.SubmitCommand(raft.CommandRequest{Command: "set x=1"})

	// ASSERT
	if !result.Accepted {
		t.Error("expected command to be accepted by leader")
	}
	if result.Duplicate {
		t.Error("expected Duplicate=false for command with no session fields")
	}
}

// §8: A duplicate request (same clientID + seqNum) that has already been applied
// must return Duplicate=true and must NOT append a new log entry.
func TestSubmitCommand_ReturnsDuplicate_WhenSameSeqNumAlreadyApplied(t *testing.T) {
	// ARRANGE
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	req := raft.CommandRequest{Command: "set x=1", ClientID: "client-1", SequenceNum: 42}

	// ACT: first submission — must be accepted.
	first := node.SubmitCommand(req)
	if !first.Accepted {
		t.Fatal("expected first submission to be accepted")
	}
	// Wait for the apply loop to process and record the session.
	time.Sleep(300 * time.Millisecond)

	// ACT: identical retry — must be a duplicate.
	second := node.SubmitCommand(req)

	// ASSERT
	if !second.Accepted {
		t.Error("duplicate command should still report Accepted=true (leader handles it)")
	}
	if !second.Duplicate {
		t.Error("expected Duplicate=true for repeated (clientID, seqNum)")
	}
}

// §8: A higher sequence number from the same client is a new command, not a duplicate.
func TestSubmitCommand_AcceptsNewSeqNum_WhenHigherThanLastApplied(t *testing.T) {
	// ARRANGE
	node := raft.New(raft.Config{
		ID:      "A",
		Peers:   grantingPeers("B", "C"),
		Storage: storage.NewMemoryStore(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	node.SubmitCommand(raft.CommandRequest{Command: "set x=1", ClientID: "client-1", SequenceNum: 1})
	time.Sleep(300 * time.Millisecond)

	// ACT: submit seq=2 from the same client.
	result := node.SubmitCommand(raft.CommandRequest{Command: "set x=2", ClientID: "client-1", SequenceNum: 2})

	// ASSERT
	if !result.Accepted {
		t.Error("expected command with higher seqNum to be accepted")
	}
	if result.Duplicate {
		t.Error("expected Duplicate=false for a new sequence number")
	}
}

// §8: Detecting a duplicate must not grow the log.
func TestSubmitCommand_DoesNotGrowLog_WhenDuplicateDetected(t *testing.T) {
	// ARRANGE
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	req := raft.CommandRequest{Command: "set x=99", ClientID: "client-1", SequenceNum: 7}
	node.SubmitCommand(req)
	time.Sleep(300 * time.Millisecond)

	logLenBefore := len(node.Snapshot().Log)

	// ACT: retry the same request.
	node.SubmitCommand(req)

	// ASSERT: log must not grow.
	logLenAfter := len(node.Snapshot().Log)
	if logLenAfter != logLenBefore {
		t.Errorf("log grew from %d to %d entries on duplicate submit; expected no growth",
			logLenBefore, logLenAfter)
	}
}

// §8: Different clients with the same sequence number are independent;
// neither is a duplicate of the other.
func TestSubmitCommand_TwoClients_SameSeqNum_AreIndependent(t *testing.T) {
	// ARRANGE
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	// Client-1 submits seq=1.
	node.SubmitCommand(raft.CommandRequest{Command: "set x=1", ClientID: "client-1", SequenceNum: 1})
	time.Sleep(300 * time.Millisecond)

	// Client-2 submits seq=1 — different client, must NOT be treated as duplicate.
	result := node.SubmitCommand(raft.CommandRequest{Command: "set y=1", ClientID: "client-2", SequenceNum: 1})

	if !result.Accepted {
		t.Error("expected command from a different client to be accepted")
	}
	if result.Duplicate {
		t.Error("same seqNum from different client must not be a duplicate")
	}
}

// §8: A follower must reject commands regardless of client session state and
// must return a leader hint.
func TestSubmitCommand_RejectsOnFollower_ReturnsLeaderHint(t *testing.T) {
	// ARRANGE: follower that knows about a leader via heartbeat.
	node := makeNode("A", nil)
	node.AppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "B"})

	// ACT
	result := node.SubmitCommand(raft.CommandRequest{
		Command:     "set x=1",
		ClientID:    "client-1",
		SequenceNum: 1,
	})

	// ASSERT
	if result.Accepted {
		t.Error("follower must not accept commands")
	}
	if result.Duplicate {
		t.Error("follower must not report Duplicate")
	}
	if result.LeaderID != "B" {
		t.Errorf("expected LeaderID redirect to B, got %q", result.LeaderID)
	}
}

// MemoryStore round-trip: ClientID and SequenceNum survive a persist/reload cycle.
func TestMemoryStore_LogEntry_PreservesClientSession_AfterRoundtrip(t *testing.T) {
	// ARRANGE
	mem := storage.NewMemoryStore()
	entries := []raft.LogEntry{
		{Term: 1, Type: raft.LogEntryCommand, Command: "set x=1", ClientID: "c1", SequenceNum: 3},
		{Term: 1, Type: raft.LogEntryNoop},
		{Term: 2, Type: raft.LogEntryCommand, Command: "set y=2", ClientID: "c2", SequenceNum: 7},
	}
	if err := mem.StoreLog(entries); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

	// ACT
	got, err := mem.GetLog(1, 4)
	if err != nil {
		t.Fatalf("GetLog: %v", err)
	}

	// ASSERT
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0].ClientID != "c1" || got[0].SequenceNum != 3 {
		t.Errorf("entry[0]: want ClientID=c1 SeqNum=3, got ClientID=%q SeqNum=%d",
			got[0].ClientID, got[0].SequenceNum)
	}
	if got[1].ClientID != "" || got[1].SequenceNum != 0 {
		t.Errorf("entry[1] (noop): expected empty ClientID SeqNum=0, got %q/%d",
			got[1].ClientID, got[1].SequenceNum)
	}
	if got[2].ClientID != "c2" || got[2].SequenceNum != 7 {
		t.Errorf("entry[2]: want ClientID=c2 SeqNum=7, got ClientID=%q SeqNum=%d",
			got[2].ClientID, got[2].SequenceNum)
	}
}

// §8: Exactly-once semantics must survive restart. After crash-style restart,
// retries of already committed requests should still be detected as duplicates.
func TestSubmitCommand_DetectsDuplicateAfterRestart_FromCommittedLogReplay(t *testing.T) {
	// ARRANGE
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	req := raft.CommandRequest{
		Command:     "set x=1",
		ClientID:    "client-1",
		SequenceNum: 11,
	}

	first := node.SubmitCommand(req)
	if !first.Accepted {
		t.Fatal("expected first command to be accepted")
	}
	waitForCommitIndex(t, node, 1, 2*time.Second)
	time.Sleep(100 * time.Millisecond) // let apply loop record session state

	// ACT: crash-style restart.
	node.Kill()
	node.Restart()
	waitForLeader(t, node, 2*time.Second)
	logLenBeforeRetry := len(node.Snapshot().Log)

	// Retry the same request after restart.
	second := node.SubmitCommand(req)

	// ASSERT
	if !second.Accepted {
		t.Fatal("expected duplicate retry to be handled by leader")
	}
	if !second.Duplicate {
		t.Fatal("expected duplicate=true after restart replay")
	}

	logLenAfterRetry := len(node.Snapshot().Log)
	if logLenAfterRetry != logLenBeforeRetry {
		t.Fatalf("duplicate retry grew log after restart: before=%d after=%d",
			logLenBeforeRetry, logLenAfterRetry)
	}
}

// ── Minimal test StateMachine ─────────────────────────────────────────────────

// testKVStore is a minimal StateMachine used by deduplication tests.
// It records applied commands so tests can verify idempotency.
type testKVStore struct {
	applied []string
}

func newTestKVStore() *testKVStore { return &testKVStore{} }

// Apply records the command and returns it as the result (for session caching).
func (k *testKVStore) Apply(cmd string) interface{} {
	k.applied = append(k.applied, cmd)
	return cmd
}

// Snapshot serialises the applied log as a newline-joined string.
func (k *testKVStore) Snapshot() (io.ReadCloser, error) {
	data := strings.Join(k.applied, "\n")
	return io.NopCloser(strings.NewReader(data)), nil
}

// Restore rebuilds the applied log from a snapshot.
func (k *testKVStore) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		k.applied = nil
		return nil
	}
	k.applied = strings.Split(string(data), "\n")
	return nil
}
