package raft

import "io"

// StateMachine is the interface that application state must implement.
//
// The Raft node calls Apply for every committed, non-noop log entry in order.
// Snapshot and Restore are called during log compaction (§7).
//
// Implementations must be safe for concurrent use; the node serialises Apply
// calls, but Snapshot may be called from a background goroutine.
type StateMachine interface {
	// Apply executes a committed log command and returns an optional result.
	//
	// The command string is the raw Command field from the log entry.
	// The return value is ignored by the Raft layer.
	Apply(command string) interface{}

	// Snapshot serialises the current state into a byte stream.
	//
	// The caller is responsible for closing the returned reader.
	// Called during log compaction; must not block Apply.
	Snapshot() (io.ReadCloser, error)

	// Restore replaces the current state from a previously created snapshot.
	//
	// Called when installing a snapshot received from the leader (§7).
	// Must be an atomic replacement: on error the previous state is preserved.
	Restore(rc io.ReadCloser) error
}
