package raft

import "time"

// StateObserver receives state change notifications from a Raft node.
//
// This interface allows external systems (like visualization gateways) to
// react to state changes without coupling the core Raft protocol to any
// particular transport or observer implementation.
//
// Implementations must be non-blocking as they are called while holding
// the node's internal lock. The observer should buffer or forward events
// asynchronously if processing is expensive.
type StateObserver interface {
	// OnStateChange is called whenever the node's observable state changes.
	//
	// The snapshot is a point-in-time copy and is safe to retain.
	// Implementations should return quickly to avoid blocking the node.
	OnStateChange(snapshot StateSnapshot)
}

// observerList is a helper for managing multiple observers.
type observerList []StateObserver

// notifyAll sends the snapshot to all observers.
// This is called while holding the node's lock, so it must be fast.
func (list observerList) notifyAll(snapshot StateSnapshot) {
	for _, obs := range list {
		obs.OnStateChange(snapshot)
	}
}

// StateSnapshotLite is a minimal state snapshot suitable for event streaming.
// It contains only the fields needed for visualization, not the full log.
type StateSnapshotLite struct {
	NodeID              string
	State               NodeState
	CurrentTerm         int64
	VotedFor            string
	CommitIndex         int64
	LastApplied         int64
	LeaderID            string
	HeartbeatIntervalMs int64
	ElectionTimeoutMs   int64
	EventTimeMs         int64
}

// ToLite converts a full StateSnapshot to a lite version for streaming.
func (s StateSnapshot) ToLite() StateSnapshotLite {
	return StateSnapshotLite{
		NodeID:              s.NodeID,
		State:               s.State,
		CurrentTerm:         s.CurrentTerm,
		VotedFor:            s.VotedFor,
		CommitIndex:         s.CommitIndex,
		LastApplied:         s.LastApplied,
		LeaderID:            s.LeaderID,
		HeartbeatIntervalMs: s.HeartbeatIntervalMs,
		ElectionTimeoutMs:   s.ElectionTimeoutMs,
		EventTimeMs:         time.Now().UnixMilli(),
	}
}
