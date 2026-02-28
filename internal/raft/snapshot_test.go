package raft_test

// Tests for the snapshot and log compaction subsystem.
//
// Coverage:
//   - TakeSnapshot captures state machine state and compacts the log
//   - InstallSnapshot restores state machine and discards log
//   - Nodes with SnapshotStore restore correctly on restart
//   - Lagging follower is brought up to date via InstallSnapshot

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/medvih/raft-core/internal/raft"
	"github.com/medvih/raft-core/internal/storage"
)

// ── TakeSnapshot ──────────────────────────────────────────────────────────────

// TakeSnapshot with no state machine configured must return an error.
func TestTakeSnapshot_ReturnsError_WhenNoStateMachineConfigured(t *testing.T) {
	node := raft.New(raft.Config{ID: "A"})

	if err := node.TakeSnapshot(); err == nil {
		t.Error("expected error when no state machine is configured")
	}
}

// TakeSnapshot must compact the in-memory log up to lastApplied.
func TestTakeSnapshot_CompactsLog_AfterApply(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go node.Run(ctx)

	// Feed all three entries in a single AppendEntries call so the leader
	// commit index can be set at the same time.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		LeaderCommit: 3,
		Entries: []raft.LogEntry{
			{Term: 1, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"x","value":"1"}`},
			{Term: 1, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"y","value":"2"}`},
			{Term: 1, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"z","value":"3"}`},
		},
	})

	// Wait for the apply loop to process all committed entries.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := kv.Get("x"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify state machine received the commands.
	if v, ok := kv.Get("x"); !ok || v != "1" {
		t.Fatalf("state machine did not apply x=1 (got %q, found=%v)", v, ok)
	}

	if err := node.TakeSnapshot(); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	snap := node.Snapshot()
	// The log must now be compacted: in-memory log should be empty (all entries
	// are covered by the snapshot).
	if snap.SnapshotIndex == 0 {
		t.Error("expected snapshotIndex > 0 after TakeSnapshot")
	}
	if len(snap.Log) != 0 {
		t.Errorf("expected empty in-memory log after full compaction, got %d entries", len(snap.Log))
	}
}

// TakeSnapshot + Restore: after a node "crashes" and is recreated with the same
// snapshot store, it should restore to the snapshotted state.
func TestTakeSnapshot_StateRestoredOnRestart(t *testing.T) {
	snapStore := storage.NewMemorySnapshotStore()

	kv1 := raft.NewKVStore()
	node1 := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv1,
		SnapshotStore: snapStore,
	})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	go node1.Run(ctx1)

	// Apply some state.
	node1.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		LeaderCommit: 2,
		Entries: []raft.LogEntry{
			{Term: 1, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"foo","value":"bar"}`},
			{Term: 1, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"baz","value":"qux"}`},
		},
	})

	// Wait for the apply loop to process entries before taking the snapshot.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := kv1.Get("foo"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := node1.TakeSnapshot(); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Simulate restart: new node with same snapshot store.
	kv2 := raft.NewKVStore()
	_ = raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv2,
		SnapshotStore: snapStore,
	})

	// kv2 should have been restored.
	if v, ok := kv2.Get("foo"); !ok || v != "bar" {
		t.Errorf("expected foo=bar after restart, got %q (found=%v)", v, ok)
	}
	if v, ok := kv2.Get("baz"); !ok || v != "qux" {
		t.Errorf("expected baz=qux after restart, got %q (found=%v)", v, ok)
	}
}

// A restart must only replay entries up to the persisted commit watermark.
// Entries above commitIndex stay unapplied even if they are durably present.
func TestRestart_ReplaysOnlyUpToPersistedCommitIndex(t *testing.T) {
	mem := storage.NewMemoryStore()
	snapStore := storage.NewMemorySnapshotStore()

	// Snapshot covers index 4.
	saveKVSnapshot(t, snapStore, 4, 1, []string{
		`{"op":"set","key":"snap","value":"v4"}`,
	})

	// Persisted log tail corresponds to absolute indexes 5, 6, 7.
	if err := mem.StoreLog([]raft.LogEntry{
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k5","value":"v5"}`},
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k6","value":"v6"}`},
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k7","value":"v7"}`},
	}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	if err := mem.SetCommitIndex(6); err != nil {
		t.Fatalf("SetCommitIndex: %v", err)
	}

	kv := raft.NewKVStore()
	node := raft.New(raft.Config{
		ID:            "A",
		Storage:       mem,
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	// Snapshot data must be present.
	if v, ok := kv.Get("snap"); !ok || v != "v4" {
		t.Fatalf("expected snapshot state snap=v4, got %q (found=%v)", v, ok)
	}
	// Committed tail entries must be replayed.
	if v, ok := kv.Get("k5"); !ok || v != "v5" {
		t.Fatalf("expected k5=v5 to be replayed, got %q (found=%v)", v, ok)
	}
	if v, ok := kv.Get("k6"); !ok || v != "v6" {
		t.Fatalf("expected k6=v6 to be replayed, got %q (found=%v)", v, ok)
	}
	// Uncommitted entry at index 7 must not be applied on restart.
	if _, ok := kv.Get("k7"); ok {
		t.Fatal("expected k7 to remain unapplied on restart")
	}

	snap := node.Snapshot()
	if snap.SnapshotIndex != 4 {
		t.Fatalf("expected snapshot index 4, got %d", snap.SnapshotIndex)
	}
	if snap.CommitIndex != 6 {
		t.Fatalf("expected commit index 6, got %d", snap.CommitIndex)
	}
	if snap.LastApplied != 6 {
		t.Fatalf("expected lastApplied 6, got %d", snap.LastApplied)
	}
}

// If commitIndex is not persisted, restart must wait for a leader commit
// update before applying entries above the snapshot boundary.
func TestRestart_WaitsForLeaderCommit_WhenCommitIndexNotPersisted(t *testing.T) {
	mem := storage.NewMemoryStore()
	snapStore := storage.NewMemorySnapshotStore()

	saveKVSnapshot(t, snapStore, 4, 1, []string{
		`{"op":"set","key":"snap","value":"v4"}`,
	})

	if err := mem.StoreLog([]raft.LogEntry{
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k5","value":"v5"}`},
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k6","value":"v6"}`},
		{Term: 2, Type: raft.LogEntryCommand, Command: `{"op":"set","key":"k7","value":"v7"}`},
	}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

	kv := raft.NewKVStore()
	node := raft.New(raft.Config{
		ID:            "A",
		Storage:       mem,
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	// Before hearing LeaderCommit, only snapshot state is applied.
	if _, ok := kv.Get("k5"); ok {
		t.Fatal("expected k5 to stay unapplied before leader commit update")
	}
	snap := node.Snapshot()
	if snap.CommitIndex != 4 {
		t.Fatalf("expected commitIndex to stay at snapshot boundary 4, got %d", snap.CommitIndex)
	}
	if snap.LastApplied != 4 {
		t.Fatalf("expected lastApplied to stay at snapshot boundary 4, got %d", snap.LastApplied)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         2,
		LeaderID:     "L",
		PrevLogIndex: 0,
		LeaderCommit: 6,
	})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if v, ok := kv.Get("k6"); ok && v == "v6" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if v, ok := kv.Get("k5"); !ok || v != "v5" {
		t.Fatalf("expected k5=v5 after leader commit update, got %q (found=%v)", v, ok)
	}
	if v, ok := kv.Get("k6"); !ok || v != "v6" {
		t.Fatalf("expected k6=v6 after leader commit update, got %q (found=%v)", v, ok)
	}
	if _, ok := kv.Get("k7"); ok {
		t.Fatal("expected k7 to remain unapplied at LeaderCommit=6")
	}

	snap = node.Snapshot()
	if snap.CommitIndex != 6 {
		t.Fatalf("expected commitIndex 6 after leader update, got %d", snap.CommitIndex)
	}
	if snap.LastApplied != 6 {
		t.Fatalf("expected lastApplied 6 after leader update, got %d", snap.LastApplied)
	}
}

// ── InstallSnapshot ───────────────────────────────────────────────────────────

// InstallSnapshot from a leader with a higher term makes the node step down.
func TestInstallSnapshot_StepsDown_WhenHigherTermReceived(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	// Advance node to term 1 by accepting a heartbeat.
	node.AppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "B"})

	snapshotData := `{"key":"value"}`
	reply := node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              5,
		LeaderID:          "C",
		LastIncludedIndex: 10,
		LastIncludedTerm:  5,
		Data:              []byte(snapshotData),
		Done:              true,
	})

	if reply.Term > 5 {
		t.Errorf("expected reply.Term <= 5, got %d", reply.Term)
	}

	snap := node.Snapshot()
	if snap.CurrentTerm < 5 {
		t.Errorf("expected term >= 5 after step-down, got %d", snap.CurrentTerm)
	}
}

// InstallSnapshot must restore the state machine and discard the log.
func TestInstallSnapshot_RestoresStateMachineAndDiscardsLog(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	// Give the node some log entries first.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "B",
		Entries:  []raft.LogEntry{{Term: 1, Command: "old"}},
	})

	// The snapshot contains a pre-built KV state.
	snapshotData := `{"restored":"true"}`
	reply := node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "B",
		LastIncludedIndex: 100,
		LastIncludedTerm:  2,
		Data:              []byte(snapshotData),
		Done:              true,
	})
	_ = reply

	// State machine must reflect the snapshot.
	val, ok := kv.Get("restored")
	if !ok || val != "true" {
		t.Errorf("expected restored=true after InstallSnapshot, got %q (found=%v)", val, ok)
	}

	snap := node.Snapshot()
	if snap.SnapshotIndex != 100 {
		t.Errorf("expected snapshotIndex=100, got %d", snap.SnapshotIndex)
	}
	if len(snap.Log) != 0 {
		t.Errorf("expected empty in-memory log after snapshot install, got %d entries", len(snap.Log))
	}
	if snap.CommitIndex != 100 {
		t.Errorf("expected commitIndex=100, got %d", snap.CommitIndex)
	}
	if snap.LastApplied != 100 {
		t.Errorf("expected lastApplied=100, got %d", snap.LastApplied)
	}
}

// InstallSnapshot with a stale term must be rejected.
func TestInstallSnapshot_Rejected_WhenTermIsStale(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	// Advance node to term 5.
	node.AppendEntries(raft.AppendEntriesArgs{Term: 5, LeaderID: "B"})

	// Send snapshot with lower term.
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "C",
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Data:              []byte(`{}`),
		Done:              true,
	})

	snap := node.Snapshot()
	// SnapshotIndex must not have advanced.
	if snap.SnapshotIndex != 0 {
		t.Errorf("stale snapshot should not be installed, snapshotIndex=%d", snap.SnapshotIndex)
	}
}

// InstallSnapshot is idempotent: re-sending the same snapshot has no effect.
func TestInstallSnapshot_IsIdempotent_WhenSameIndexSentTwice(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	args := raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte(`{"a":"1"}`),
		Done:              true,
	}
	node.InstallSnapshot(args)
	// Overwrite the KV store manually to verify second call is a no-op.
	kv.Apply(`{"op":"set","key":"a","value":"changed"}`)

	// Re-send same snapshot; should be ignored.
	node.InstallSnapshot(args)

	// State should still be the manually-changed value, not the snapshot value.
	val, _ := kv.Get("a")
	if val != "changed" {
		t.Errorf("expected a=changed (idempotent), got %q", val)
	}
}

// ── Auto-snapshot threshold ───────────────────────────────────────────────────

// When SnapshotThreshold is reached, an automatic snapshot is taken.
func TestAutoSnapshot_TriggeredWhenThresholdReached(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:                "A",
		StateMachine:      kv,
		SnapshotStore:     snapStore,
		SnapshotThreshold: 3, // Snapshot after 3 applied entries.
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.Run(ctx)

	// Feed entries via AppendEntries (simulating a leader).
	for i := 0; i < 4; i++ {
		node.AppendEntries(raft.AppendEntriesArgs{
			Term:         1,
			LeaderID:     "B",
			PrevLogIndex: int64(i),
			PrevLogTerm:  1,
			Entries:      []raft.LogEntry{{Term: 1, Command: `{"op":"set","key":"k","value":"v"}`}},
			LeaderCommit: int64(i + 1),
		})
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for the auto-snapshot goroutine to run.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap := node.Snapshot()
		if snap.SnapshotIndex > 0 {
			return // Success.
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("automatic snapshot was not taken within deadline")
}

// ── Chunked snapshot transfer ─────────────────────────────────────────────────

// InstallSnapshot must assemble multiple chunks before installing.
func TestInstallSnapshot_ChunkedTransfer_InstallsOnFinalChunk(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	fullData := []byte(`{"assembled":"yes"}`)
	half := len(fullData) / 2
	chunk1 := fullData[:half]
	chunk2 := fullData[half:]

	// Arrange: first chunk — no installation yet.
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              chunk1,
		Done:              false,
	})

	// Assert: not installed after first chunk.
	snap := node.Snapshot()
	if snap.SnapshotIndex != 0 {
		t.Errorf("snapshot installed prematurely after first chunk, index=%d", snap.SnapshotIndex)
	}

	// Act: send final chunk.
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Offset:            int64(half),
		Data:              chunk2,
		Done:              true,
	})

	// Assert: snapshot installed after final chunk.
	snap = node.Snapshot()
	if snap.SnapshotIndex != 50 {
		t.Errorf("expected snapshotIndex=50 after final chunk, got %d", snap.SnapshotIndex)
	}
	if _, ok := kv.Get("assembled"); !ok {
		t.Error("state machine did not restore after chunked install")
	}
}

// InstallSnapshot must discard a partial transfer when an out-of-order chunk arrives.
func TestInstallSnapshot_ChunkedTransfer_Resets_WhenOffsetMismatch(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	data := []byte(`{"should":"not_install"}`)

	// Send first chunk — offset 0, not done.
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 20,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              data[:4],
		Done:              false,
	})

	// Send a second chunk with wrong offset (simulates a dropped chunk).
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 20,
		LastIncludedTerm:  1,
		Offset:            99, // wrong — should be 4
		Data:              data[4:],
		Done:              true,
	})

	// Snapshot must not be installed.
	snap := node.Snapshot()
	if snap.SnapshotIndex != 0 {
		t.Errorf("snapshot should not be installed on offset mismatch, got index=%d", snap.SnapshotIndex)
	}
}

// TakeSnapshot must embed the current ClusterConfig in SnapshotMeta.
func TestTakeSnapshot_PreservesClusterConfig(t *testing.T) {
	kv := raft.NewKVStore()
	snapStore := storage.NewMemorySnapshotStore()

	// Arrange: a node that acts as a follower.
	node := raft.New(raft.Config{
		ID:            "A",
		StateMachine:  kv,
		SnapshotStore: snapStore,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go node.Run(ctx)

	// Install a snapshot that includes a ClusterConfig so clusterConfig is set.
	cfg := &raft.ClusterConfig{
		Members: []raft.ClusterMember{
			{ID: "A", Role: raft.Voter},
			{ID: "B", Role: raft.Voter},
		},
	}
	node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "B",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte(`{"seeded":"yes"}`),
		Done:              true,
		Configuration:     cfg,
	})

	// Append one more entry after the snapshot base.
	node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      []raft.LogEntry{{Term: 1, Command: `{"op":"set","key":"extra","value":"1"}`}},
		LeaderCommit: 6,
	})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := kv.Get("extra"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Act: take a new snapshot.
	if err := node.TakeSnapshot(); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Assert: the saved snapshot contains the ClusterConfig.
	meta, _, err := snapStore.Load()
	if err != nil {
		t.Fatalf("Load snapshot: %v", err)
	}
	if meta.Configuration == nil {
		t.Error("expected ClusterConfig embedded in SnapshotMeta, got nil")
	}
	if len(meta.Configuration.Members) != 2 {
		t.Errorf("expected 2 members in config, got %d", len(meta.Configuration.Members))
	}
}

// saveKVSnapshot applies commands to a temporary KV store and persists the
// resulting snapshot payload with the provided metadata.
func saveKVSnapshot(t *testing.T, snapStore *storage.MemorySnapshotStore, lastIndex, lastTerm int64, commands []string) {
	t.Helper()

	source := raft.NewKVStore()
	for _, cmd := range commands {
		source.Apply(cmd)
	}

	rc, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll snapshot: %v", err)
	}

	if err := snapStore.Save(raft.SnapshotMeta{LastIndex: lastIndex, LastTerm: lastTerm}, data); err != nil {
		t.Fatalf("Save snapshot: %v", err)
	}
}
