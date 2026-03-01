package storage

import (
	"testing"

	"github.com/mehdiakiki/raft-core/internal/raft"
)

func TestBoltStore_TermPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and set term.
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.SetTerm(5)
	if err != nil {
		t.Fatalf("failed to set term: %v", err)
	}
	store.Close()

	// Reopen store and verify term persisted.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	term, err := store.GetTerm()
	if err != nil {
		t.Fatalf("failed to get term: %v", err)
	}
	if term != 5 {
		t.Errorf("expected term 5, got %d", term)
	}
}

func TestBoltStore_VotedForPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and set votedFor.
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.SetVotedFor("node-A")
	if err != nil {
		t.Fatalf("failed to set votedFor: %v", err)
	}
	store.Close()

	// Reopen store and verify votedFor persisted.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	votedFor, err := store.GetVotedFor()
	if err != nil {
		t.Fatalf("failed to get votedFor: %v", err)
	}
	if votedFor != "node-A" {
		t.Errorf("expected votedFor 'node-A', got %q", votedFor)
	}
}

func TestBoltStore_CommitIndexPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and set commit index.
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.SetCommitIndex(9)
	if err != nil {
		t.Fatalf("failed to set commit index: %v", err)
	}
	store.Close()

	// Reopen store and verify commit index persisted.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	commitIndex, err := store.GetCommitIndex()
	if err != nil {
		t.Fatalf("failed to get commit index: %v", err)
	}
	if commitIndex != 9 {
		t.Errorf("expected commit index 9, got %d", commitIndex)
	}
}

func TestBoltStore_LogPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and append log entries.
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	entries := []raft.LogEntry{
		{Term: 1, Command: "set x=1"},
		{Term: 1, Command: "set y=2"},
	}
	err = store.AppendLog(entries)
	if err != nil {
		t.Fatalf("failed to append log: %v", err)
	}
	store.Close()

	// Reopen store and verify log persisted.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("failed to get log: %v", err)
	}
	if len(log) != 2 {
		t.Errorf("expected 2 entries, got %d", len(log))
	}
	if log[0].Command != "set x=1" {
		t.Errorf("expected first command 'set x=1', got %q", log[0].Command)
	}
}

func TestBoltStore_LogTruncation(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Append 3 entries.
	entries := []raft.LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 2, Command: "c"},
	}
	store.AppendLog(entries)

	// Truncate after index 1.
	err = store.TruncateLog(2)
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	// Verify only 1 entry remains.
	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("failed to get log: %v", err)
	}
	if len(log) != 1 {
		t.Errorf("expected 1 entry, got %d", len(log))
	}
	if log[0].Command != "a" {
		t.Errorf("expected command 'a', got %q", log[0].Command)
	}

	store.Close()
}

func TestBoltStore_LastLogIndexAndTerm(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Empty log.
	index, term := store.LastLogIndexAndTerm()
	if index != 0 || term != 0 {
		t.Errorf("expected (0, 0), got (%d, %d)", index, term)
	}

	// Append entries.
	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "a"},
	})

	index, term = store.LastLogIndexAndTerm()
	if index != 1 || term != 1 {
		t.Errorf("expected (1, 1), got (%d, %d)", index, term)
	}

	store.AppendLog([]raft.LogEntry{
		{Term: 2, Command: "b"},
	})

	index, term = store.LastLogIndexAndTerm()
	if index != 2 || term != 2 {
		t.Errorf("expected (2, 2), got (%d, %d)", index, term)
	}
}

func TestBoltStore_Close(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

// ── StoreLog tests ────────────────────────────────────────────────────────────

// StoreLog with empty slice clears the log, and survives a reopen.
func TestBoltStore_StoreLog_ClearsExisting(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	store.AppendLog([]raft.LogEntry{{Term: 1, Command: "a"}})

	if err := store.StoreLog([]raft.LogEntry{}); err != nil {
		t.Fatalf("StoreLog returned error: %v", err)
	}
	store.Close()

	// Reopen and verify log is empty.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	index, _ := store.LastLogIndexAndTerm()
	if index != 0 {
		t.Errorf("expected empty log, got index %d", index)
	}
}

// StoreLog replaces existing entries and the new entries survive a reopen.
func TestBoltStore_StoreLog_Replaces(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "old-1"},
		{Term: 1, Command: "old-2"},
		{Term: 1, Command: "old-3"},
	})

	newEntries := []raft.LogEntry{
		{Term: 2, Command: "new-1"},
		{Term: 2, Command: "new-2"},
	}
	if err := store.StoreLog(newEntries); err != nil {
		t.Fatalf("StoreLog returned error: %v", err)
	}
	store.Close()

	// Reopen and verify only the new entries exist.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer store.Close()

	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(log) != 2 {
		t.Fatalf("expected 2 entries after StoreLog, got %d", len(log))
	}
	if log[0].Command != "new-1" {
		t.Errorf("log[0] = %q, want \"new-1\"", log[0].Command)
	}
	if log[1].Command != "new-2" {
		t.Errorf("log[1] = %q, want \"new-2\"", log[1].Command)
	}
}

// StoreLog is idempotent: calling it twice with the same entries
// must not produce duplicate entries on disk.
func TestBoltStore_StoreLog_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	entries := []raft.LogEntry{{Term: 1, Command: "x"}}

	store.StoreLog(entries)
	store.StoreLog(entries) // second call must not duplicate

	index, _ := store.LastLogIndexAndTerm()
	if index != 1 {
		t.Errorf("expected log length 1 after two identical StoreLogs, got %d", index)
	}
}

// LogEntryType (noop vs command) must survive a BoltDB close+reopen cycle.
// This verifies that the binary encoding includes the Type byte.
func TestBoltStore_LogEntryType_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	entries := []raft.LogEntry{
		{Term: 1, Type: raft.LogEntryNoop, Command: ""},
		{Term: 1, Type: raft.LogEntryCommand, Command: "set x=42"},
	}
	if err := store.StoreLog(entries); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	store.Close()

	// Reopen and verify types are preserved.
	store, err = NewBoltStore(tmpDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()

	got, err := store.GetLog(1, 3)
	if err != nil {
		t.Fatalf("GetLog: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Type != raft.LogEntryNoop {
		t.Errorf("entry[0].Type = %v, want LogEntryNoop", got[0].Type)
	}
	if got[1].Type != raft.LogEntryCommand {
		t.Errorf("entry[1].Type = %v, want LogEntryCommand", got[1].Type)
	}
	if got[1].Command != "set x=42" {
		t.Errorf("entry[1].Command = %q, want \"set x=42\"", got[1].Command)
	}
}
