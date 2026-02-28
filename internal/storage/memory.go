// Package storage provides storage implementations for the Raft consensus algorithm.
package storage

import (
	"github.com/medvih/raft-core/internal/raft"
)

// MemoryStore is an in-memory implementation of raft.Storage.
// It is safe for concurrent use by multiple goroutines.
//
// This implementation is useful for:
// - Testing the Raft implementation
// - Demos where persistence is not required
// - Scenarios where in-memory storage is preferred
type MemoryStore struct {
	currentTerm int64
	votedFor    string
	commitIndex int64
	log         []raft.LogEntry
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// GetTerm implements raft.Storage.
func (s *MemoryStore) GetTerm() (int64, error) {
	return s.currentTerm, nil
}

// SetTerm implements raft.Storage.
func (s *MemoryStore) SetTerm(term int64) error {
	s.currentTerm = term
	return nil
}

// GetVotedFor implements raft.Storage.
func (s *MemoryStore) GetVotedFor() (string, error) {
	return s.votedFor, nil
}

// SetVotedFor implements raft.Storage.
func (s *MemoryStore) SetVotedFor(id string) error {
	s.votedFor = id
	return nil
}

// GetCommitIndex implements raft.Storage.
func (s *MemoryStore) GetCommitIndex() (int64, error) {
	return s.commitIndex, nil
}

// SetCommitIndex implements raft.Storage.
func (s *MemoryStore) SetCommitIndex(index int64) error {
	s.commitIndex = index
	return nil
}

// StoreLog implements raft.Storage.
// It atomically replaces the entire log with the provided entries.
func (s *MemoryStore) StoreLog(entries []raft.LogEntry) error {
	copied := make([]raft.LogEntry, len(entries))
	copy(copied, entries)
	s.log = copied
	return nil
}

// AppendLog appends entries to the end of the log.
// Not part of the raft.Storage interface; provided for tests and
// incremental construction of log state.
func (s *MemoryStore) AppendLog(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.log = append(s.log, entries...)
	return nil
}

// GetLog implements raft.Storage.
func (s *MemoryStore) GetLog(start, end int64) ([]raft.LogEntry, error) {
	if start >= end {
		return nil, nil
	}

	startIdx := int(start) - 1
	if startIdx < 0 {
		startIdx = 0
	}

	endIdx := int(end) - 1
	if endIdx > len(s.log) {
		endIdx = len(s.log)
	}

	if startIdx >= endIdx {
		return nil, nil
	}

	result := make([]raft.LogEntry, endIdx-startIdx)
	copy(result, s.log[startIdx:endIdx])
	return result, nil
}

// TruncateLog implements raft.Storage.
func (s *MemoryStore) TruncateLog(index int64) error {
	if index <= 0 {
		return nil
	}
	truncateIdx := int(index) - 1
	if truncateIdx < len(s.log) {
		s.log = s.log[:truncateIdx]
	}
	return nil
}

// LastLogIndexAndTerm implements raft.Storage.
func (s *MemoryStore) LastLogIndexAndTerm() (int64, int64) {
	if len(s.log) == 0 {
		return 0, 0
	}
	lastIdx := len(s.log) - 1
	return int64(lastIdx + 1), s.log[lastIdx].Term
}

// Close implements raft.Storage.
// MemoryStore has no resources to close, so this is a no-op.
func (s *MemoryStore) Close() error {
	return nil
}

// Compile-time check that MemoryStore implements raft.Storage.
var _ raft.Storage = (*MemoryStore)(nil)
