package raft

import "testing"

// recoveryStorage is a minimal in-memory Storage used by recovery unit tests.
type recoveryStorage struct {
	term        int64
	votedFor    string
	commitIndex int64
	log         []LogEntry
}

func (s *recoveryStorage) GetTerm() (int64, error)        { return s.term, nil }
func (s *recoveryStorage) SetTerm(term int64) error       { s.term = term; return nil }
func (s *recoveryStorage) GetVotedFor() (string, error)   { return s.votedFor, nil }
func (s *recoveryStorage) SetVotedFor(id string) error    { s.votedFor = id; return nil }
func (s *recoveryStorage) GetCommitIndex() (int64, error) { return s.commitIndex, nil }
func (s *recoveryStorage) SetCommitIndex(index int64) error {
	s.commitIndex = index
	return nil
}

func (s *recoveryStorage) StoreLog(entries []LogEntry) error {
	copied := make([]LogEntry, len(entries))
	copy(copied, entries)
	s.log = copied
	return nil
}

func (s *recoveryStorage) GetLog(start, end int64) ([]LogEntry, error) {
	if start >= end {
		return nil, nil
	}

	startPos := start - 1
	if startPos < 0 {
		startPos = 0
	}
	endPos := end - 1
	if endPos > int64(len(s.log)) {
		endPos = int64(len(s.log))
	}
	if startPos >= endPos {
		return nil, nil
	}

	out := make([]LogEntry, endPos-startPos)
	copy(out, s.log[startPos:endPos])
	return out, nil
}

func (s *recoveryStorage) TruncateLog(index int64) error {
	if index <= 0 {
		return nil
	}
	cut := index - 1
	if cut < int64(len(s.log)) {
		s.log = s.log[:cut]
	}
	return nil
}

func (s *recoveryStorage) LastLogIndexAndTerm() (int64, int64) {
	if len(s.log) == 0 {
		return 0, 0
	}
	last := len(s.log) - 1
	return int64(last + 1), s.log[last].Term
}

func (s *recoveryStorage) Close() error { return nil }

func TestReconstructConfigFromLog_IgnoresUncommittedConfigEntry(t *testing.T) {
	configData, err := encodeConfig(&ClusterConfig{
		Members: []ClusterMember{
			{ID: "A", Role: Voter},
			{ID: "B", Role: Voter},
		},
	})
	if err != nil {
		t.Fatalf("encodeConfig: %v", err)
	}

	store := &recoveryStorage{
		log: []LogEntry{
			{Term: 1, Type: LogEntryConfig, ConfigData: configData},
		},
		commitIndex: 0, // Config entry is present but uncommitted.
	}

	node := New(Config{ID: "A", Storage: store})

	if node.clusterConfig != nil {
		t.Fatalf("expected clusterConfig to stay nil for uncommitted config entry, got %+v", node.clusterConfig)
	}
}

func TestReconstructConfigFromLog_UsesCommittedConfigEntry(t *testing.T) {
	want := &ClusterConfig{
		Members: []ClusterMember{
			{ID: "A", Role: Voter},
			{ID: "B", Role: NonVoter},
		},
	}
	configData, err := encodeConfig(want)
	if err != nil {
		t.Fatalf("encodeConfig: %v", err)
	}

	store := &recoveryStorage{
		log: []LogEntry{
			{Term: 1, Type: LogEntryConfig, ConfigData: configData},
		},
		commitIndex: 1, // Config entry is committed.
	}

	node := New(Config{ID: "A", Storage: store})

	if node.clusterConfig == nil {
		t.Fatal("expected clusterConfig to be reconstructed from committed config entry")
	}
	if len(node.clusterConfig.Members) != len(want.Members) {
		t.Fatalf("expected %d members, got %d", len(want.Members), len(node.clusterConfig.Members))
	}
	for i, member := range want.Members {
		got := node.clusterConfig.Members[i]
		if got.ID != member.ID || got.Role != member.Role {
			t.Fatalf("member[%d] = %+v, want %+v", i, got, member)
		}
	}
}
