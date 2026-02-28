package raft

// lastLogIndexAndTerm returns the absolute index and term of the last log entry.
// Returns (snapshotIndex, snapshotTerm) when the in-memory log is empty.
//
// §5.4.1: Used when starting an election to advertise the candidate's log
// position to other servers.
//
// n must NOT be nil.  Caller must hold n.mu.
func (n *Node) lastLogIndexAndTerm() (index, term int64) {
	if len(n.log) == 0 {
		return n.snapshotIndex, n.snapshotTerm
	}
	last := len(n.log) - 1
	return n.snapshotIndex + int64(last+1), n.log[last].Term
}

// logTermAt returns the term of the log entry at the given 1-based absolute index.
// Returns 0 if the index is out of range or has been compacted away.
//
// Caller must hold n.mu.
func (n *Node) logTermAt(index int64) int64 {
	if index <= 0 {
		return 0
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	// Convert absolute index to in-memory slice position.
	pos := index - n.snapshotIndex - 1
	if pos < 0 || pos >= int64(len(n.log)) {
		return 0
	}
	return n.log[pos].Term
}

// isLogUpToDate returns true if a candidate's log is at least as up-to-date
// as the receiver's log.
//
// §5.4.1: "If the logs have last entries with different terms, then the log
// with the later term is more up-to-date. If logs end with the same term,
// then whichever log is longer is more up-to-date."
//
// Caller must hold n.mu.
func (n *Node) isLogUpToDate(candidateLastIndex, candidateLastTerm int64) bool {
	localLastIndex, localLastTerm := n.lastLogIndexAndTerm()

	if candidateLastTerm != localLastTerm {
		return candidateLastTerm > localLastTerm
	}
	return candidateLastIndex >= localLastIndex
}

// logSliceFrom returns a copy of entries starting at the given absolute index.
// Returns nil if startIndex is beyond the end of the log.
//
// Caller must hold n.mu.
func (n *Node) logSliceFrom(startIndex int64) []LogEntry {
	pos := startIndex - n.snapshotIndex - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= int64(len(n.log)) {
		return nil
	}
	result := make([]LogEntry, int64(len(n.log))-pos)
	copy(result, n.log[pos:])
	return result
}

// logLen returns the absolute index of the last log entry (equal to
// snapshotIndex + len(n.log)).
//
// Caller must hold n.mu.
func (n *Node) logLen() int64 {
	return n.snapshotIndex + int64(len(n.log))
}
