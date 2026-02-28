package raft

// NodeState represents the current role of a Raft node.
//
// §5.1: "At any given time, each server is in one of three states:
// leader, follower, or candidate."
//
// We add a fourth state, Dead, to simulate node failures for testing.
type NodeState int

const (
	// Follower is the initial state of all nodes.
	// Followers respond to RPCs from leaders and candidates.
	// If a follower receives no communication for a while, it becomes a candidate.
	Follower NodeState = iota

	// Candidate is the state during leader election.
	// A candidate requests votes from other nodes.
	// If it receives votes from a majority, it becomes leader.
	Candidate

	// Leader is the state that handles all client requests.
	// The leader replicates log entries to followers.
	// There is at most one leader per term.
	Leader

	// Dead is a simulated node failure state.
	// Dead nodes ignore all RPCs.
	// This is used for testing failover scenarios.
	Dead
)

// String returns a human-readable representation of the state.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	case Dead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// canVote returns true if a node in this state can grant votes.
// Only followers and candidates can vote (leaders never vote for others).
func (s NodeState) canVote() bool {
	return s == Follower || s == Candidate
}

// isActive returns true if the node is not dead.
func (s NodeState) isActive() bool {
	return s != Dead
}
