package storage

import (
	"testing"

	"github.com/medvih/raft-core/internal/raft"
)

func TestMemoryStore_GetTerm_Empty(t *testing.T) {
	store := NewMemoryStore()

	term, err := store.GetTerm()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 0 {
		t.Errorf("expected term 0, got %d", term)
	}
}

func TestMemoryStore_SetTerm(t *testing.T) {
	store := NewMemoryStore()

	err := store.SetTerm(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	term, err := store.GetTerm()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 5 {
		t.Errorf("expected term 5, got %d", term)
	}
}

func TestMemoryStore_GetCommitIndex_Empty(t *testing.T) {
	store := NewMemoryStore()

	commitIndex, err := store.GetCommitIndex()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if commitIndex != 0 {
		t.Errorf("expected commitIndex 0, got %d", commitIndex)
	}
}

func TestMemoryStore_SetCommitIndex(t *testing.T) {
	store := NewMemoryStore()

	err := store.SetCommitIndex(7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	commitIndex, err := store.GetCommitIndex()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if commitIndex != 7 {
		t.Errorf("expected commitIndex 7, got %d", commitIndex)
	}
}

func TestMemoryStore_GetVotedFor_Empty(t *testing.T) {
	store := NewMemoryStore()

	votedFor, err := store.GetVotedFor()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if votedFor != "" {
		t.Errorf("expected empty votedFor, got %q", votedFor)
	}
}

func TestMemoryStore_SetVotedFor(t *testing.T) {
	store := NewMemoryStore()

	err := store.SetVotedFor("node-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	votedFor, err := store.GetVotedFor()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if votedFor != "node-A" {
		t.Errorf("expected votedFor 'node-A', got %q", votedFor)
	}
}

func TestMemoryStore_AppendLog(t *testing.T) {
	store := NewMemoryStore()

	entries := []raft.LogEntry{
		{Term: 1, Command: "set x=1"},
		{Term: 1, Command: "set y=2"},
	}
	err := store.AppendLog(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	log, err := store.GetLog(1, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(log) != 2 {
		t.Errorf("expected 2 entries, got %d", len(log))
	}
	if log[0].Command != "set x=1" {
		t.Errorf("expected first command 'set x=1', got %q", log[0].Command)
	}
	if log[1].Command != "set y=2" {
		t.Errorf("expected second command 'set y=2', got %q", log[1].Command)
	}
}

func TestMemoryStore_AppendLog_Empty(t *testing.T) {
	store := NewMemoryStore()

	err := store.AppendLog(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = store.AppendLog([]raft.LogEntry{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	index, term := store.LastLogIndexAndTerm()
	if index != 0 || term != 0 {
		t.Errorf("expected empty log, got index=%d term=%d", index, term)
	}
}

func TestMemoryStore_GetLog(t *testing.T) {
	store := NewMemoryStore()

	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 2, Command: "c"},
	})

	tests := []struct {
		name      string
		start     int64
		end       int64
		wantLen   int
		wantFirst string
	}{
		{"full log", 1, 4, 3, "a"},
		{"first two", 1, 3, 2, "a"},
		{"last two", 2, 4, 2, "b"},
		{"single entry", 2, 3, 1, "b"},
		{"start beyond end", 5, 10, 0, ""},
		{"start >= end", 3, 1, 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := store.GetLog(tt.start, tt.end)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(log) != tt.wantLen {
				t.Errorf("got %d entries, want %d", len(log), tt.wantLen)
			}
			if tt.wantLen > 0 && log[0].Command != tt.wantFirst {
				t.Errorf("first command = %q, want %q", log[0].Command, tt.wantFirst)
			}
		})
	}
}

func TestMemoryStore_TruncateLog(t *testing.T) {
	store := NewMemoryStore()

	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 2, Command: "c"},
	})

	err := store.TruncateLog(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(log) != 1 {
		t.Errorf("expected 1 entry after truncate, got %d", len(log))
	}
	if log[0].Command != "a" {
		t.Errorf("expected command 'a', got %q", log[0].Command)
	}
}

func TestMemoryStore_TruncateLog_ZeroIndex(t *testing.T) {
	store := NewMemoryStore()

	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "a"},
	})

	err := store.TruncateLog(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(log) != 1 {
		t.Errorf("expected 1 entry, got %d", len(log))
	}
}

func TestMemoryStore_LastLogIndexAndTerm(t *testing.T) {
	store := NewMemoryStore()

	index, term := store.LastLogIndexAndTerm()
	if index != 0 || term != 0 {
		t.Errorf("expected empty log (0, 0), got (%d, %d)", index, term)
	}

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

func TestMemoryStore_Close(t *testing.T) {
	store := NewMemoryStore()

	err := store.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── StoreLog tests ────────────────────────────────────────────────────────────

// StoreLog with an empty slice clears the log.
func TestMemoryStore_StoreLog_ClearsExisting(t *testing.T) {
	store := NewMemoryStore()
	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
	})

	err := store.StoreLog([]raft.LogEntry{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	index, _ := store.LastLogIndexAndTerm()
	if index != 0 {
		t.Errorf("expected empty log after StoreLog([]), got index %d", index)
	}
}

// StoreLog replaces existing entries atomically.
func TestMemoryStore_StoreLog_Replaces(t *testing.T) {
	store := NewMemoryStore()
	store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: "old-1"},
		{Term: 1, Command: "old-2"},
		{Term: 1, Command: "old-3"},
	})

	newEntries := []raft.LogEntry{
		{Term: 2, Command: "new-1"},
		{Term: 2, Command: "new-2"},
	}
	err := store.StoreLog(newEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	log, err := store.GetLog(1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(log))
	}
	if log[0].Command != "new-1" {
		t.Errorf("expected command \"new-1\", got %q", log[0].Command)
	}
	if log[1].Command != "new-2" {
		t.Errorf("expected command \"new-2\", got %q", log[1].Command)
	}
}

// StoreLog is idempotent: calling it twice with the same entries
// must not duplicate entries.
func TestMemoryStore_StoreLog_Idempotent(t *testing.T) {
	store := NewMemoryStore()
	entries := []raft.LogEntry{
		{Term: 1, Command: "x"},
	}

	store.StoreLog(entries)
	store.StoreLog(entries) // second call must not duplicate

	index, _ := store.LastLogIndexAndTerm()
	if index != 1 {
		t.Errorf("expected log length 1 after two identical StoreLogs, got %d", index)
	}
}

// LogEntryType (noop vs command) must be preserved by MemoryStore.
func TestMemoryStore_LogEntryType_RoundTrip(t *testing.T) {
	store := NewMemoryStore()
	entries := []raft.LogEntry{
		{Term: 1, Type: raft.LogEntryNoop, Command: ""},
		{Term: 1, Type: raft.LogEntryCommand, Command: "set x=42"},
	}
	if err := store.StoreLog(entries); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

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
}
