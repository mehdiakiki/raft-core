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

import "time"

// Configuration defaults for a Raft node.
//
// These values are chosen to balance responsiveness (quick leader election)
// with stability (avoiding unnecessary elections).
//
// §5.2: "Election timeouts are chosen randomly from a fixed interval
// (e.g., 150-300ms) to reduce the likelihood of split votes."
const (
	// ElectionTimeoutMin is the minimum time a follower waits before
	// starting an election. This should be several times larger than
	// the heartbeat interval to avoid unnecessary elections.
	ElectionTimeoutMin = 150 * time.Millisecond

	// ElectionTimeoutMax is the maximum time a follower waits before
	// starting an election. The actual timeout is randomized within
	// [ElectionTimeoutMin, ElectionTimeoutMax].
	ElectionTimeoutMax = 300 * time.Millisecond

	// HeartbeatInterval is how often the leader sends AppendEntries RPCs
	// to followers. This must be less than ElectionTimeoutMin.
	//
	// §5.2: "The leader sends periodic heartbeats (AppendEntries RPCs with
	// no log entries) to all followers to maintain authority."
	HeartbeatInterval = 50 * time.Millisecond
)

// Config holds the configuration for a Raft node.
//
// All fields have sensible defaults, but can be overridden for testing
// or specific deployment requirements.
type Config struct {
	// ID is the unique identifier for this node.
	// Must be unique within the cluster.
	ID string

	// Peers maps peer node IDs to their Peer interface.
	// The node uses this to send RPCs to other nodes.
	Peers map[string]Peer

	// Storage is the storage backend for persisting Raft state.
	// If nil, a MemoryStore will be used.
	Storage Storage

	// StateMachine is the application state machine.
	// If non-nil, committed log entries are forwarded to StateMachine.Apply.
	// Required for TakeSnapshot and InstallSnapshot.
	StateMachine StateMachine

	// SnapshotStore persists snapshots across restarts.
	// Required for TakeSnapshot and InstallSnapshot.
	SnapshotStore SnapshotStore

	// SnapshotThreshold is the number of applied log entries that triggers
	// an automatic snapshot.  0 disables automatic snapshots.
	SnapshotThreshold int64

	// ElectionTimeoutMin overrides the default minimum election timeout.
	// Use 0 for default value.
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax overrides the default maximum election timeout.
	// Use 0 for default value.
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval overrides the default heartbeat interval.
	// Use 0 for default value.
	HeartbeatInterval time.Duration
}

// electionTimeoutMin returns the configured minimum election timeout,
// or the default if not set.
func (c Config) electionTimeoutMin() time.Duration {
	if c.ElectionTimeoutMin > 0 {
		return c.ElectionTimeoutMin
	}
	return ElectionTimeoutMin
}

// electionTimeoutMax returns the configured maximum election timeout,
// or the default if not set.
func (c Config) electionTimeoutMax() time.Duration {
	if c.ElectionTimeoutMax > 0 {
		return c.ElectionTimeoutMax
	}
	return ElectionTimeoutMax
}

// heartbeatInterval returns the configured heartbeat interval,
// or the default if not set.
func (c Config) heartbeatInterval() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return HeartbeatInterval
}

// randomElectionTimeout returns a random duration in
// [electionTimeoutMin, electionTimeoutMax].
//
// §5.2: "Randomization helps avoid split votes by ensuring that not all
// candidates start elections at the same time."
func (c Config) randomElectionTimeout() time.Duration {
	min := int64(c.electionTimeoutMin())
	max := int64(c.electionTimeoutMax())
	spread := max - min
	return time.Duration(min + randomInt64(spread))
}
