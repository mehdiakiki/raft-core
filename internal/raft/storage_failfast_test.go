package raft

import (
	"errors"
	"testing"
)

type failingTermStore struct {
	*memoryStorePlaceholder
}

func (s *failingTermStore) SetTerm(_ int64) error {
	return errors.New("forced SetTerm failure")
}

func TestRequestVote_Panics_WhenTermPersistenceFails(t *testing.T) {
	store := &failingTermStore{memoryStorePlaceholder: &memoryStorePlaceholder{}}
	node := New(Config{ID: "n1", Storage: store})

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on fatal term persistence failure")
		}
	}()

	_ = node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
}
