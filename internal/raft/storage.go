// Package raft implements the Raft consensus algorithm.
//
// Raft is a consensus algorithm for managing a replicated log. It produces the
// same result as Paxos, but is easier to understand.
//
// This implementation follows the Raft paper (Ongaro & Ousterhout, 2014):
// https://raft.github.io/raft.pdf
//
// All section references (§5.2, §5.3, etc.) throughout this package refer to
// that paper.
package raft

// Storage persists Raft state to stable storage.
//
// §5.1: "Raft persists state to stable storage in the same order it
// responds to RPCs, which simplifies the Raft implementation."
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Storage interface {
	// GetTerm returns the current term, or 0 if never set.
	GetTerm() (int64, error)

	// SetTerm persists the current term.
	// Must be called before responding to RPCs with the new term.
	SetTerm(term int64) error

	// GetVotedFor returns the candidate ID this node voted for in the current term.
	// Returns empty string if no vote has been cast.
	GetVotedFor() (string, error)

	// SetVotedFor persists the vote.
	// Must be called before responding to RequestVote RPCs.
	SetVotedFor(id string) error

	// GetCommitIndex returns the highest log index known to be committed.
	// Returns 0 if no commit watermark has been persisted yet.
	GetCommitIndex() (int64, error)

	// SetCommitIndex persists the highest committed index.
	// Implementations may assume this value is monotonic.
	SetCommitIndex(index int64) error

	// StoreLog atomically replaces the entire log with the provided entries.
	// Used by persistLog to write the in-memory log to stable storage
	// without accumulating duplicate entries across calls.
	StoreLog(entries []LogEntry) error

	// GetLog returns log entries in the range [start, end).
	// start is 1-indexed (first entry is at index 1).
	// Returns nil if start >= end or start is beyond the log.
	GetLog(start, end int64) ([]LogEntry, error)

	// TruncateLog removes entries from index onwards.
	// Used when a leader discovers conflicting entries (§5.3).
	TruncateLog(index int64) error

	// LastLogIndexAndTerm returns the index and term of the last log entry.
	// Returns (0, 0) if the log is empty.
	LastLogIndexAndTerm() (index int64, term int64)

	// Close releases any resources held by the storage.
	Close() error
}
