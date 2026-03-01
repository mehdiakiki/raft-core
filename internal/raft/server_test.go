package raft_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/mehdiakiki/raft-core/gen/raft"
	"github.com/mehdiakiki/raft-core/internal/raft"
	"github.com/mehdiakiki/raft-core/internal/storage"
)

// SetAlive(true) must perform crash-style recovery by reloading durable state.
func TestSetAlive_RestartReloadsFromStorage(t *testing.T) {
	mem := storage.NewMemoryStore()
	node := raft.New(raft.Config{ID: "A", Storage: mem})
	server := raft.NewServer(node)

	// Build some initial in-memory state.
	node.RequestVote(raft.RequestVoteArgs{
		Term:        2,
		CandidateID: "B",
	})
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         2,
		LeaderID:     "L",
		Entries:      []raft.LogEntry{{Term: 2, Type: raft.LogEntryCommand, Command: "old"}},
		LeaderCommit: 1,
	})

	killReply, err := server.SetAlive(context.Background(), &pb.SetAliveRequest{Alive: false})
	if err != nil {
		t.Fatalf("SetAlive(false): %v", err)
	}
	if killReply.Alive {
		t.Fatal("expected node to report not alive after kill")
	}

	// Simulate external durable state updates while node is down.
	if err := mem.SetTerm(9); err != nil {
		t.Fatalf("SetTerm: %v", err)
	}
	if err := mem.SetVotedFor("Z"); err != nil {
		t.Fatalf("SetVotedFor: %v", err)
	}
	if err := mem.StoreLog([]raft.LogEntry{
		{Term: 9, Type: raft.LogEntryCommand, Command: "persisted-1"},
		{Term: 9, Type: raft.LogEntryCommand, Command: "persisted-2"},
	}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	if err := mem.SetCommitIndex(2); err != nil {
		t.Fatalf("SetCommitIndex: %v", err)
	}

	restartReply, err := server.SetAlive(context.Background(), &pb.SetAliveRequest{Alive: true})
	if err != nil {
		t.Fatalf("SetAlive(true): %v", err)
	}
	if !restartReply.Alive {
		t.Fatal("expected node to report alive after restart")
	}

	snap := node.Snapshot()
	if snap.State.String() != "FOLLOWER" {
		t.Fatalf("expected FOLLOWER after restart, got %s", snap.State)
	}
	if snap.CurrentTerm != 9 {
		t.Fatalf("expected term 9 after reload, got %d", snap.CurrentTerm)
	}
	if snap.VotedFor != "Z" {
		t.Fatalf("expected votedFor=Z after reload, got %q", snap.VotedFor)
	}
	if len(snap.Log) != 2 {
		t.Fatalf("expected 2 log entries after reload, got %d", len(snap.Log))
	}
	if snap.Log[0].Command != "persisted-1" || snap.Log[1].Command != "persisted-2" {
		t.Fatalf("unexpected log after reload: %+v", snap.Log)
	}
	if snap.CommitIndex != 2 {
		t.Fatalf("expected commitIndex 2 after reload, got %d", snap.CommitIndex)
	}
}

// SubmitCommand should report committed=true once the entry is applied.
func TestSubmitCommand_ReportsCommittedAfterApply(t *testing.T) {
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	server := raft.NewServer(node)
	reply, err := server.SubmitCommand(context.Background(), &pb.SubmitCommandRequest{
		Command:     "set x=1",
		ClientId:    "client-1",
		SequenceNum: 1,
	})
	if err != nil {
		t.Fatalf("SubmitCommand: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected success=true")
	}
	if !reply.Committed {
		t.Fatal("expected committed=true")
	}
	if reply.Duplicate {
		t.Fatal("expected duplicate=false for first submit")
	}
}

// Duplicate retries should remain committed and return the cached result.
func TestSubmitCommand_DuplicateReplyIncludesCachedResult(t *testing.T) {
	kvStore := newTestKVStore()
	node := raft.New(raft.Config{
		ID:           "A",
		Peers:        grantingPeers("B", "C"),
		Storage:      storage.NewMemoryStore(),
		StateMachine: kvStore,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go node.Run(ctx)
	waitForLeader(t, node, 2*time.Second)

	server := raft.NewServer(node)
	req := &pb.SubmitCommandRequest{
		Command:     "set x=1",
		ClientId:    "client-1",
		SequenceNum: 7,
	}

	first, err := server.SubmitCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("first SubmitCommand: %v", err)
	}
	if !first.Success || !first.Committed {
		t.Fatalf("expected first request to be success+committed, got %+v", first)
	}

	second, err := server.SubmitCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("second SubmitCommand: %v", err)
	}
	if !second.Success {
		t.Fatal("expected success=true for duplicate request")
	}
	if !second.Committed {
		t.Fatal("expected committed=true for duplicate request")
	}
	if !second.Duplicate {
		t.Fatal("expected duplicate=true for duplicate request")
	}
	if second.Result != "set x=1" {
		t.Fatalf("expected cached result 'set x=1', got %q", second.Result)
	}
}
