package raft

import "sync/atomic"

// MetricsSnapshot is a copy of the node's protocol counters.
//
// These counters are monotonic for the process lifetime and are safe to read
// without holding Node.mu.
type MetricsSnapshot struct {
	// ElectionsStarted increments when the node enters Candidate.
	ElectionsStarted int64

	// ElectionsWon increments when the node transitions Candidate -> Leader.
	ElectionsWon int64

	// CommandsSubmitted increments when the leader appends a client command.
	CommandsSubmitted int64

	// CommandsApplied increments when a command entry is applied.
	CommandsApplied int64

	// LogEntriesReplicated increments by entries acked by followers.
	LogEntriesReplicated int64

	// ReadIndexRequests increments on each non-empty ReadIndex call.
	ReadIndexRequests int64
}

// Metrics holds atomic protocol counters.
//
// Keep this focused on small core counters used by operators and tests.
type Metrics struct {
	electionsStarted     atomic.Int64
	electionsWon         atomic.Int64
	commandsSubmitted    atomic.Int64
	commandsApplied      atomic.Int64
	logEntriesReplicated atomic.Int64
	readIndexRequests    atomic.Int64
}

func newMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) incElectionsStarted() {
	if m != nil {
		m.electionsStarted.Add(1)
	}
}

func (m *Metrics) incElectionsWon() {
	if m != nil {
		m.electionsWon.Add(1)
	}
}

func (m *Metrics) incCommandsSubmitted() {
	if m != nil {
		m.commandsSubmitted.Add(1)
	}
}

func (m *Metrics) incCommandsApplied() {
	if m != nil {
		m.commandsApplied.Add(1)
	}
}

func (m *Metrics) addLogEntriesReplicated(delta int64) {
	if m != nil && delta > 0 {
		m.logEntriesReplicated.Add(delta)
	}
}

func (m *Metrics) incReadIndexRequests() {
	if m != nil {
		m.readIndexRequests.Add(1)
	}
}

func (m *Metrics) snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		ElectionsStarted:     m.electionsStarted.Load(),
		ElectionsWon:         m.electionsWon.Load(),
		CommandsSubmitted:    m.commandsSubmitted.Load(),
		CommandsApplied:      m.commandsApplied.Load(),
		LogEntriesReplicated: m.logEntriesReplicated.Load(),
		ReadIndexRequests:    m.readIndexRequests.Load(),
	}
}
