package storage_test

// Tests for MemorySnapshotStore.
//
// Coverage:
//   - Save stores the snapshot data and metadata
//   - Load returns the stored snapshot
//   - Load returns ErrNoSnapshot when empty
//   - Save with subsequent Load returns the latest snapshot

import (
	"bytes"
	"testing"

	"github.com/medvih/raft-core/internal/raft"
	"github.com/medvih/raft-core/internal/storage"
)

// Load on an empty store must return ErrNoSnapshot.
func TestMemorySnapshotStore_Load_ReturnsErrNoSnapshot_WhenEmpty(t *testing.T) {
	s := storage.NewMemorySnapshotStore()

	_, _, err := s.Load()
	if err != raft.ErrNoSnapshot {
		t.Errorf("expected ErrNoSnapshot, got %v", err)
	}
}

// Save must persist the metadata and data so Load can retrieve them.
func TestMemorySnapshotStore_Save_PersistsData(t *testing.T) {
	s := storage.NewMemorySnapshotStore()

	meta := raft.SnapshotMeta{LastIndex: 42, LastTerm: 3}
	data := []byte(`{"key":"value"}`)

	if err := s.Save(meta, data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	gotMeta, gotData, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if gotMeta.LastIndex != 42 {
		t.Errorf("expected LastIndex=42, got %d", gotMeta.LastIndex)
	}
	if gotMeta.LastTerm != 3 {
		t.Errorf("expected LastTerm=3, got %d", gotMeta.LastTerm)
	}
	if !bytes.Equal(gotData, data) {
		t.Errorf("expected data %q, got %q", data, gotData)
	}
}

// A second Save must replace the first so Load always returns the latest.
func TestMemorySnapshotStore_Save_ReplacesExistingSnapshot(t *testing.T) {
	s := storage.NewMemorySnapshotStore()

	s.Save(raft.SnapshotMeta{LastIndex: 1, LastTerm: 1}, []byte("old"))

	newer := []byte("newer-data")
	if err := s.Save(raft.SnapshotMeta{LastIndex: 10, LastTerm: 2}, newer); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	gotMeta, gotData, err := s.Load()
	if err != nil {
		t.Fatalf("Load after second Save: %v", err)
	}
	if gotMeta.LastIndex != 10 {
		t.Errorf("expected LastIndex=10, got %d", gotMeta.LastIndex)
	}
	if !bytes.Equal(gotData, newer) {
		t.Errorf("expected %q, got %q", newer, gotData)
	}
}

// Load must return a copy of the data, not a reference to the internal buffer.
func TestMemorySnapshotStore_Load_ReturnsCopy(t *testing.T) {
	s := storage.NewMemorySnapshotStore()

	original := []byte("snapshot")
	s.Save(raft.SnapshotMeta{LastIndex: 1, LastTerm: 1}, original)

	_, gotData, _ := s.Load()
	// Mutate the returned slice.
	gotData[0] = 'X'

	// A second Load must still return the original data.
	_, gotData2, _ := s.Load()
	if gotData2[0] == 'X' {
		t.Error("Load returned a reference to internal buffer, not a copy")
	}
}
