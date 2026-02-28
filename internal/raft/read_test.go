package raft

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// singleLeader builds a single-node cluster that becomes leader
// on its own. Returns the node and a cancel func that stops it.
func singleLeader(t *testing.T) (*Node, context.CancelFunc) {
	t.Helper()
	// Use two granting peers so the election quorum is met immediately.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &grantingPeer{},
			"n3": &grantingPeer{},
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		StateMachine:       &trackingKV{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go node.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		s := node.state
		node.mu.Unlock()
		if s == Leader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	node.mu.Lock()
	if node.state != Leader {
		node.mu.Unlock()
		cancel()
		t.Fatal("node did not become leader in time")
	}
	node.mu.Unlock()
	return node, cancel
}

// trackingKV is a minimal StateMachine that records applied commands.
type trackingKV struct {
	applied []string
}

func (k *trackingKV) Apply(cmd string) interface{} {
	k.applied = append(k.applied, cmd)
	return cmd
}
func (k *trackingKV) Snapshot() (io.ReadCloser, error) { return io.NopCloser(nil), nil }
func (k *trackingKV) Restore(_ io.ReadCloser) error    { return nil }

// failingPeer always returns an error, so quorum is never reached.
type failingPeer struct{}

func (f *failingPeer) RequestVote(_ context.Context, _ RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, fmt.Errorf("unreachable")
}
func (f *failingPeer) AppendEntries(_ context.Context, _ AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, fmt.Errorf("unreachable")
}
func (f *failingPeer) PreVote(_ context.Context, _ PreVoteArgs) (PreVoteReply, error) {
	return PreVoteReply{}, fmt.Errorf("unreachable")
}

// grantingPeer always votes yes and acknowledges AppendEntries, allowing a
// node to win elections and replicate log entries in unit tests.
type grantingPeer struct{}

func (g *grantingPeer) RequestVote(_ context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{VoteGranted: true, Term: args.Term}, nil
}
func (g *grantingPeer) AppendEntries(_ context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{Success: true, Term: args.Term}, nil
}
func (g *grantingPeer) PreVote(_ context.Context, args PreVoteArgs) (PreVoteReply, error) {
	return PreVoteReply{VoteGranted: true, Term: args.NextTerm}, nil
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestReadIndex_ReturnsSuccess_WhenNodeIsLeader verifies that a single-node
// leader confirms its own leadership and returns a valid read index.
func TestReadIndex_ReturnsSuccess_WhenNodeIsLeader(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()
	ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ctxCancel()

	// Act
	result := node.ReadIndex(ctx, "read-1", Linearizable)

	// Assert
	if !result.Success {
		t.Fatalf("expected Success=true, got %+v", result)
	}
	if result.ReadIndex < 0 {
		t.Fatalf("expected non-negative ReadIndex, got %d", result.ReadIndex)
	}
}

// TestReadIndex_ReturnsFailure_WhenNodeIsFollower verifies that a follower
// rejects the read and does not return Success.
func TestReadIndex_ReturnsFailure_WhenNodeIsFollower(t *testing.T) {
	// Arrange: follower with long timeout so it never starts an election.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &failingPeer{},
		},
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	result := node.ReadIndex(ctx, "read-1", Linearizable)

	// Assert
	if result.Success {
		t.Fatalf("expected Success=false for follower, got %+v", result)
	}
}

// TestReadIndex_ReturnsFailure_WhenReadIDIsEmpty verifies that an empty readID
// is rejected without panicking.
func TestReadIndex_ReturnsFailure_WhenReadIDIsEmpty(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
	defer ctxCancel()

	// Act
	result := node.ReadIndex(ctx, "", Linearizable)

	// Assert
	if result.Success {
		t.Fatalf("expected Success=false for empty readID, got %+v", result)
	}
}

// TestReadIndex_IsLinearizable_AfterWrite verifies that a ReadIndex issued
// after a committed write returns a readIndex that has been applied by the
// state machine, satisfying linearizability (SS8).
func TestReadIndex_IsLinearizable_AfterWrite(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()

	submitResult := node.SubmitCommand(CommandRequest{Command: "set x=42"})
	if !submitResult.Accepted {
		t.Fatal("expected command to be accepted by leader")
	}

	// Wait until at least one entry is applied.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		applied := node.lastApplied
		node.mu.Unlock()
		if applied >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ctxCancel()

	// Act
	result := node.ReadIndex(ctx, "read-after-write", Linearizable)

	// Assert
	if !result.Success {
		t.Fatalf("expected Success=true, got %+v", result)
	}
	node.mu.Lock()
	lastApplied := node.lastApplied
	node.mu.Unlock()
	if result.ReadIndex > lastApplied {
		t.Fatalf("readIndex %d > lastApplied %d: state machine not caught up",
			result.ReadIndex, lastApplied)
	}
}

// TestReadIndex_CancelledContext_ReturnsFailure verifies that cancelling the
// context while waiting for a quorum causes ReadIndex to return promptly.
func TestReadIndex_CancelledContext_ReturnsFailure(t *testing.T) {
	// Arrange: force leader state with unreachable peers so quorum never forms.
	node := New(Config{
		ID: "leader",
		Peers: map[string]Peer{
			"p1": &failingPeer{},
			"p2": &failingPeer{},
		},
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
	})
	node.mu.Lock()
	node.state = Leader
	node.leaderID = node.id
	node.nextIndex = map[string]int64{"p1": 1, "p2": 1}
	node.matchIndex = map[string]int64{"p1": 0, "p2": 0}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	start := time.Now()
	result := node.ReadIndex(ctx, "read-cancel", Linearizable)

	// Assert
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("ReadIndex did not respect context cancellation in time")
	}
	if result.Success {
		t.Fatalf("expected Success=false when context cancelled, got %+v", result)
	}
}

// TestReadIndex_MultipleReads_SameLeader verifies that concurrent reads with
// distinct IDs all succeed on a single-node leader.
func TestReadIndex_MultipleReads_SameLeader(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()

	const count = 5
	results := make([]ReadIndexResult, count)
	done := make(chan int, count)
	ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ctxCancel()

	// Act
	for i := 0; i < count; i++ {
		go func(i int) {
			results[i] = node.ReadIndex(ctx, fmt.Sprintf("read-%d", i), Linearizable)
			done <- i
		}(i)
	}
	for i := 0; i < count; i++ {
		<-done
	}

	// Assert
	for i, r := range results {
		if !r.Success {
			t.Errorf("read %d: expected Success=true, got %+v", i, r)
		}
	}
}

// ── Gap 6: ConsistencyLevel ───────────────────────────────────────────────────

// TestReadIndex_LeaderLocal_ReturnsSuccess_WhenNodeIsLeader verifies that the
// LeaderLocal path succeeds without a heartbeat round on a single-node leader.
func TestReadIndex_LeaderLocal_ReturnsSuccess_WhenNodeIsLeader(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()
	ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ctxCancel()

	// Act
	result := node.ReadIndex(ctx, "read-ll", LeaderLocal)

	// Assert
	if !result.Success {
		t.Fatalf("expected Success=true for LeaderLocal, got %+v", result)
	}
}

// TestReadIndex_LeaderLocal_ReturnsFailure_WhenNodeIsFollower verifies that a
// follower rejects LeaderLocal reads and provides a redirect hint.
func TestReadIndex_LeaderLocal_ReturnsFailure_WhenNodeIsFollower(t *testing.T) {
	// Arrange: follower with a long election timeout so it never becomes leader.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &failingPeer{},
		},
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	result := node.ReadIndex(ctx, "read-ll-follower", LeaderLocal)

	// Assert
	if result.Success {
		t.Fatalf("expected Success=false for LeaderLocal on follower, got %+v", result)
	}
}

// TestReadIndex_Any_ReturnsSuccess_OnAnyNode verifies that the Any path
// succeeds immediately on both leaders and followers.
func TestReadIndex_Any_ReturnsSuccess_OnAnyNode(t *testing.T) {
	// Arrange: plain follower node — Any must not require leadership.
	node := New(Config{
		ID: "n1",
		Peers: map[string]Peer{
			"n2": &failingPeer{},
		},
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	result := node.ReadIndex(ctx, "read-any", Any)

	// Assert: Any always succeeds regardless of role.
	if !result.Success {
		t.Fatalf("expected Success=true for Any, got %+v", result)
	}
}

// TestReadIndex_Any_ReturnsCurrentLastApplied verifies that ReadIndex with Any
// returns the node's current lastApplied without blocking.
func TestReadIndex_Any_ReturnsCurrentLastApplied(t *testing.T) {
	// Arrange
	node := New(Config{
		ID:                 "n1",
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
	})
	// Manually set lastApplied to a known value.
	node.mu.Lock()
	node.lastApplied = 42
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	result := node.ReadIndex(ctx, "read-any-idx", Any)

	// Assert
	if !result.Success {
		t.Fatalf("expected Success=true, got %+v", result)
	}
	if result.ReadIndex != 42 {
		t.Fatalf("expected ReadIndex=42, got %d", result.ReadIndex)
	}
}

// TestAdvancePendingReads_CountsUniquePeersOnly verifies that repeated
// acknowledgements from the same peer do not inflate quorum.
func TestAdvancePendingReads_CountsUniquePeersOnly(t *testing.T) {
	node := New(Config{
		ID: "leader",
		Peers: map[string]Peer{
			"p1": &grantingPeer{},
			"p2": &grantingPeer{},
			"p3": &grantingPeer{},
			"p4": &grantingPeer{},
		},
	})

	node.mu.Lock()
	node.state = Leader
	node.leaderID = node.id
	node.nextIndex = map[string]int64{"p1": 1, "p2": 1, "p3": 1, "p4": 1}
	node.matchIndex = map[string]int64{"p1": 0, "p2": 0, "p3": 0, "p4": 0}
	pr := node.registerPendingRead(context.Background(), "read-unique")
	round := pr.requiredRound
	node.advancePendingReads("p1", round)
	node.advancePendingReads("p1", round) // duplicate peer ack; must not count twice

	select {
	case <-pr.leadershipConfirmed:
		node.mu.Unlock()
		t.Fatal("read confirmed prematurely from duplicate acks by one peer")
	default:
	}

	node.advancePendingReads("p2", round) // second unique peer => quorum with self

	select {
	case <-pr.leadershipConfirmed:
	default:
		node.mu.Unlock()
		t.Fatal("expected read to confirm after second unique peer ack")
	}
	node.mu.Unlock()
}

// TestAdvancePendingReads_IgnoresOldRounds verifies that heartbeat acks from
// rounds before requiredRound are ignored.
func TestAdvancePendingReads_IgnoresOldRounds(t *testing.T) {
	node := New(Config{
		ID: "leader",
		Peers: map[string]Peer{
			"p1": &grantingPeer{},
		},
	})

	node.mu.Lock()
	node.state = Leader
	node.leaderID = node.id
	node.nextIndex = map[string]int64{"p1": 1}
	node.matchIndex = map[string]int64{"p1": 0}
	pr := node.registerPendingRead(context.Background(), "read-round")
	round := pr.requiredRound
	node.advancePendingReads("p1", round-1) // stale ack: must be ignored

	select {
	case <-pr.leadershipConfirmed:
		node.mu.Unlock()
		t.Fatal("read confirmed from stale heartbeat round")
	default:
	}

	node.advancePendingReads("p1", round)
	select {
	case <-pr.leadershipConfirmed:
	default:
		node.mu.Unlock()
		t.Fatal("expected read to confirm on required heartbeat round")
	}
	node.mu.Unlock()
}

// ── Gap 8: Read sweep ─────────────────────────────────────────────────────────

// TestSweepStalePendingReads_RemovesEntriesWithCancelledContext verifies that
// sweepStalePendingReads cleans up entries whose caller context is already done.
func TestSweepStalePendingReads_RemovesEntriesWithCancelledContext(t *testing.T) {
	// Arrange: build a node that is the leader.
	node, cancel := singleLeader(t)
	defer cancel()

	// Register a pending read with a pre-cancelled context.
	cancelledCtx, cancelCtxFunc := context.WithCancel(context.Background())
	cancelCtxFunc() // cancel immediately

	node.mu.Lock()
	node.pendingReads["stale-read"] = &pendingRead{
		ctx:                 cancelledCtx,
		readIndex:           node.commitIndex,
		requiredRound:       0,
		quorumCount:         0,
		ackedPeers:          map[string]struct{}{},
		majority:            3,
		leadershipConfirmed: make(chan struct{}),
	}
	initialLen := len(node.pendingReads)
	node.mu.Unlock()

	if initialLen == 0 {
		t.Fatal("expected at least one pending read before sweep")
	}

	// Act
	node.mu.Lock()
	node.sweepStalePendingReads()
	afterLen := len(node.pendingReads)
	node.mu.Unlock()

	// Assert: the stale entry must have been removed.
	if afterLen >= initialLen {
		t.Errorf("expected fewer pending reads after sweep; before=%d after=%d",
			initialLen, afterLen)
	}
}

// TestSweepStalePendingReads_KeepsConfirmedReads verifies that entries whose
// leadershipConfirmed channel is already closed are not swept (they are waiting
// for waitForApply to finish, which will clean them up naturally).
func TestSweepStalePendingReads_KeepsConfirmedReads(t *testing.T) {
	// Arrange
	node, cancel := singleLeader(t)
	defer cancel()

	confirmedCh := make(chan struct{})
	close(confirmedCh) // already confirmed

	liveCtx, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()

	node.mu.Lock()
	node.pendingReads["confirmed-read"] = &pendingRead{
		ctx:                 liveCtx,
		readIndex:           node.commitIndex,
		requiredRound:       0,
		quorumCount:         3,
		ackedPeers:          map[string]struct{}{node.id: {}},
		majority:            3,
		leadershipConfirmed: confirmedCh,
	}
	node.mu.Unlock()

	// Act
	node.mu.Lock()
	node.sweepStalePendingReads()
	_, kept := node.pendingReads["confirmed-read"]
	node.mu.Unlock()

	// Assert: confirmed entry must not be swept.
	if !kept {
		t.Error("expected confirmed pending read to be kept after sweep")
	}
}
