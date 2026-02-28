package storage

import (
	"sync"

	"github.com/medvih/raft-core/internal/raft"
)

// MemorySnapshotStore is an in-memory implementation of raft.SnapshotStore.
//
// It retains only the single most recent snapshot, which is sufficient for
// testing and for environments where durability across restarts is not required.
// For production use, replace this with a file-backed implementation.
type MemorySnapshotStore struct {
	mu   sync.RWMutex
	meta raft.SnapshotMeta
	data []byte
	has  bool
}

// NewMemorySnapshotStore returns an empty MemorySnapshotStore.
func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{}
}

// Save implements raft.SnapshotStore.
// Atomically replaces any previous snapshot with the new one.
func (s *MemorySnapshotStore) Save(meta raft.SnapshotMeta, data []byte) error {
	copied := make([]byte, len(data))
	copy(copied, data)

	s.mu.Lock()
	s.meta = meta
	s.data = copied
	s.has = true
	s.mu.Unlock()
	return nil
}

// Load implements raft.SnapshotStore.
// Returns ErrNoSnapshot if no snapshot has been saved yet.
func (s *MemorySnapshotStore) Load() (raft.SnapshotMeta, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.has {
		return raft.SnapshotMeta{}, nil, raft.ErrNoSnapshot
	}

	copied := make([]byte, len(s.data))
	copy(copied, s.data)
	return s.meta, copied, nil
}

// Compile-time check: MemorySnapshotStore implements raft.SnapshotStore.
var _ raft.SnapshotStore = (*MemorySnapshotStore)(nil)
