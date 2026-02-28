package raft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Node is a single participant in a Raft cluster.
//
// All exported methods are safe to call concurrently.
// The node can be in one of four states: Follower, Candidate, Leader, or Dead.
type Node struct {
	// Configuration
	config Config

	// Storage for persisting Raft state
	storage Storage

	// Synchronization
	mu sync.Mutex

	// Network
	id    string
	peers map[string]Peer

	// ── Persistent State (§5.1, Figure 2) ─────────────────────────────────────
	// These fields are persisted to storage before responding to RPCs.
	currentTerm int64
	votedFor    string
	log         []LogEntry

	// ── Volatile State (§5.1, Figure 2) ───────────────────────────────────────
	commitIndex int64
	lastApplied int64
	state       NodeState
	leaderID    string

	// ── Leader Volatile State (§5.3, Figure 2) ────────────────────────────────
	// Reinitialized after election.
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// ── Snapshot State (§7) ───────────────────────────────────────────────────
	// snapshotIndex is the index of the last log entry included in the latest
	// snapshot (0 if no snapshot has been taken).
	snapshotIndex int64
	// snapshotTerm is the term of the log entry at snapshotIndex.
	snapshotTerm int64

	// ── Election Tracking ──────────────────────────────────────────────────────
	votesReceived int

	// lastHeartbeat is the time the most recent valid AppendEntries was received
	// from the current leader.  Used by PreVote voters to refuse pre-votes when
	// they believe a healthy leader still exists (§3.1).
	lastHeartbeat time.Time

	// electionTimeout is the currently active randomized election timeout for
	// this timer cycle. It is refreshed each cycle in runElectionTimer.
	electionTimeout time.Duration

	// ── Internal Channels ──────────────────────────────────────────────────────────
	electionResetCh chan struct{} // Signaled to reset election timer
	applyReadyCh    chan struct{} // Signaled when new entries are committed
	done            chan struct{} // Closed when node should stop

	// ── Client Sessions (§8) ───────────────────────────────────────────────────────
	// clientSessions tracks the last applied command per client for deduplication.
	// Keyed by client ID; values hold the last sequence number and its result.
	clientSessions map[string]ClientSession

	// ── Linearizable Reads (§8) ────────────────────────────────────────────────────
	// pendingReads holds in-flight ReadIndex requests waiting for leadership
	// confirmation via a heartbeat quorum.  Keyed by an opaque read ID string.
	pendingReads map[string]*pendingRead

	// heartbeatRound increments every time the leader sends a heartbeat batch.
	// ReadIndex requests capture a requiredRound so only newer acks can satisfy
	// their quorum.
	heartbeatRound uint64

	// ── Membership (§6) ────────────────────────────────────────────────────────────
	// clusterConfig is the latest committed cluster configuration.
	// nil means all peers are treated as full Voters (bootstrapped cluster).
	clusterConfig *ClusterConfig

	// ── Snapshot Receive State (§7) ────────────────────────────────────────────────
	// pendingSnapshot accumulates in-flight chunked InstallSnapshot data from
	// the leader.  nil when no snapshot transfer is in progress.
	pendingSnapshot *pendingSnapshotState

	// ── Observability ───────────────────────────────────────────────────────────────
	// metrics tracks protocol counters for operations and diagnostics.
	metrics *Metrics

	// ── Public Output ──────────────────────────────────────────────────────────────
	StateCh chan StateSnapshot // Buffered channel for state updates
}

// New creates a new Raft node.
//
// The node starts as a Follower in term 0 with an empty log.
// The storage is loaded with any persisted state.
// Call Run to start the node's background goroutines.
func New(config Config) *Node {
	if config.ID == "" {
		panic("raft: config.ID is required")
	}
	if config.Peers == nil {
		config.Peers = make(map[string]Peer)
	}

	// Use MemoryStore if no storage is provided.
	storage := config.Storage
	if storage == nil {
		storage = &memoryStorePlaceholder{}
	}

	node := &Node{
		config:          config,
		storage:         storage,
		id:              config.ID,
		peers:           config.Peers,
		state:           Follower,
		electionResetCh: make(chan struct{}, 1),
		applyReadyCh:    make(chan struct{}, 1),
		done:            make(chan struct{}),
		StateCh:         make(chan StateSnapshot, 64),
		clientSessions:  make(map[string]ClientSession),
		pendingReads:    make(map[string]*pendingRead),
		metrics:         newMetrics(),
	}

	// Load persisted state from storage.
	node.loadFromStorage()

	return node
}

// memoryStorePlaceholder is used when no storage is provided.
// This is a simple in-memory implementation for the default case.
type memoryStorePlaceholder struct {
	mu          sync.RWMutex
	currentTerm int64
	votedFor    string
	commitIndex int64
	log         []LogEntry
}

func (m *memoryStorePlaceholder) GetTerm() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTerm, nil
}

func (m *memoryStorePlaceholder) SetTerm(term int64) error {
	m.mu.Lock()
	m.currentTerm = term
	m.mu.Unlock()
	return nil
}

func (m *memoryStorePlaceholder) GetVotedFor() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.votedFor, nil
}

func (m *memoryStorePlaceholder) SetVotedFor(id string) error {
	m.mu.Lock()
	m.votedFor = id
	m.mu.Unlock()
	return nil
}

func (m *memoryStorePlaceholder) GetCommitIndex() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.commitIndex, nil
}

func (m *memoryStorePlaceholder) SetCommitIndex(index int64) error {
	m.mu.Lock()
	m.commitIndex = index
	m.mu.Unlock()
	return nil
}

func (m *memoryStorePlaceholder) StoreLog(entries []LogEntry) error {
	copied := make([]LogEntry, len(entries))
	copy(copied, entries)

	m.mu.Lock()
	m.log = copied
	m.mu.Unlock()
	return nil
}

func (m *memoryStorePlaceholder) GetLog(start, end int64) ([]LogEntry, error) {
	if start >= end {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	startIdx := int(start) - 1
	if startIdx < 0 {
		startIdx = 0
	}
	endIdx := int(end) - 1
	if endIdx > len(m.log) {
		endIdx = len(m.log)
	}
	if startIdx >= endIdx {
		return nil, nil
	}

	result := make([]LogEntry, endIdx-startIdx)
	copy(result, m.log[startIdx:endIdx])
	return result, nil
}

func (m *memoryStorePlaceholder) TruncateLog(index int64) error {
	if index <= 0 {
		return nil
	}

	truncateIdx := int(index) - 1

	m.mu.Lock()
	if truncateIdx < len(m.log) {
		m.log = m.log[:truncateIdx]
	}
	m.mu.Unlock()
	return nil
}

func (m *memoryStorePlaceholder) LastLogIndexAndTerm() (int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.log) == 0 {
		return 0, 0
	}
	lastIdx := len(m.log) - 1
	return int64(lastIdx + 1), m.log[lastIdx].Term
}

func (m *memoryStorePlaceholder) Close() error { return nil }

// loadFromStorage loads persisted state from storage into memory.
func (n *Node) loadFromStorage() {
	// Load term.
	term, err := n.storage.GetTerm()
	if err != nil {
		slog.Error("failed to load term from storage", "id", n.id, "err", err)
		return
	}
	n.currentTerm = term

	// Load votedFor.
	votedFor, err := n.storage.GetVotedFor()
	if err != nil {
		slog.Error("failed to load votedFor from storage", "id", n.id, "err", err)
		return
	}
	n.votedFor = votedFor

	// Load log.
	lastIndex, _ := n.storage.LastLogIndexAndTerm()
	if lastIndex > 0 {
		logEntries, err := n.storage.GetLog(1, lastIndex+1)
		if err != nil {
			slog.Error("failed to load log from storage", "id", n.id, "err", err)
			return
		}
		n.log = logEntries
	}

	// Restore from snapshot if one exists.
	if n.config.SnapshotStore != nil && n.config.StateMachine != nil {
		n.loadSnapshotFromStore()
	}

	// Recover the persisted commit watermark. Entries above this index are
	// uncommitted and must not be replayed on startup.
	n.loadCommitIndexFromStorage()

	// §5.3: Replay any log entries that were committed in a previous run but
	// not yet reflected in a snapshot.
	n.replayCommittedEntries()

	// §6: Restore the latest cluster configuration from the log so membership
	// is correct immediately on startup without waiting for the apply loop.
	n.reconstructConfigFromLog()

	slog.Info("loaded state from storage",
		"id", n.id,
		"term", n.currentTerm,
		"votedFor", n.votedFor,
		"logLength", len(n.log),
		"snapshotIndex", n.snapshotIndex,
	)
}

// loadSnapshotFromStore restores the state machine from the most recent
// persisted snapshot and updates in-memory compaction bookkeeping.
//
// Called from loadFromStorage; caller must not hold n.mu.
func (n *Node) loadSnapshotFromStore() {
	meta, data, err := n.config.SnapshotStore.Load()
	if err == ErrNoSnapshot {
		return // No snapshot yet, that's fine.
	}
	if err != nil {
		slog.Error("failed to load snapshot from store", "id", n.id, "err", err)
		return
	}

	if err := n.config.StateMachine.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		slog.Error("failed to restore state machine from snapshot", "id", n.id, "err", err)
		return
	}

	n.snapshotIndex = meta.LastIndex
	n.snapshotTerm = meta.LastTerm

	// Entries covered by the snapshot are already applied.
	n.commitIndex = meta.LastIndex
	if meta.LastIndex > n.lastApplied {
		n.lastApplied = meta.LastIndex
	}

	// Restore cluster config captured in the snapshot metadata.
	if meta.Configuration != nil {
		members := make([]ClusterMember, len(meta.Configuration.Members))
		copy(members, meta.Configuration.Members)
		n.clusterConfig = &ClusterConfig{Members: members}
	}

	// Discard any in-memory log entries whose index is ≤ snapshotIndex.
	n.compactLogUpTo(meta.LastIndex, meta.LastTerm)

	slog.Info("state restored from snapshot",
		"id", n.id,
		"snapshotIndex", n.snapshotIndex,
		"snapshotTerm", n.snapshotTerm,
	)
}

// loadCommitIndexFromStorage restores commitIndex and clamps it to the safe
// interval [snapshotIndex, logLen].
//
// Called from loadFromStorage; caller must not hold n.mu.
func (n *Node) loadCommitIndexFromStorage() {
	persistedCommit, err := n.storage.GetCommitIndex()
	if err != nil {
		slog.Error("failed to load commitIndex from storage", "id", n.id, "err", err)
		return
	}

	if persistedCommit < n.snapshotIndex {
		persistedCommit = n.snapshotIndex
	}

	lastLogIndex := n.logLen()
	if persistedCommit > lastLogIndex {
		persistedCommit = lastLogIndex
	}

	n.commitIndex = persistedCommit
}

// replayCommittedEntries applies log entries in (lastApplied, commitIndex].
//
// Called from loadFromStorage; caller must not hold n.mu.
func (n *Node) replayCommittedEntries() {
	if n.config.StateMachine == nil {
		return
	}

	for n.lastApplied < n.commitIndex {
		n.lastApplied++

		memPos := n.lastApplied - n.snapshotIndex - 1
		if memPos < 0 || memPos >= int64(len(n.log)) {
			continue // Covered by the snapshot.
		}

		entry := n.log[memPos]
		if entry.Type == LogEntryNoop {
			continue // No-ops advance lastApplied but are not forwarded.
		}

		// Rebuild deduplication state from committed command entries so §8
		// exactly-once behavior survives crash-style restarts.
		if entry.Type == LogEntryCommand {
			var result interface{}
			result = n.config.StateMachine.Apply(entry.Command)
			n.recordSession(entry.ClientID, entry.SequenceNum, result)
			continue
		}

		// Config entries are replayed into in-memory membership by
		// reconstructConfigFromLog; they are never forwarded to the state machine.
		if entry.Type == LogEntryConfig {
			continue
		}
	}

	if n.lastApplied > n.snapshotIndex {
		slog.Info("replayed committed entries on startup",
			"id", n.id,
			"lastApplied", n.lastApplied,
			"commitIndex", n.commitIndex,
		)
	}
}

// persistTerm persists the current term to storage.
//
// Caller must hold n.mu.
func (n *Node) persistTerm() {
	if err := n.storage.SetTerm(n.currentTerm); err != nil {
		n.failStorageWrite("term", err)
	}
}

// persistVotedFor persists the votedFor field to storage.
//
// Caller must hold n.mu.
func (n *Node) persistVotedFor() {
	if err := n.storage.SetVotedFor(n.votedFor); err != nil {
		n.failStorageWrite("votedFor", err)
	}
}

// persistLog persists the current in-memory log to storage.
//
// Uses StoreLog to atomically replace whatever is on disk with the
// current in-memory slice, avoiding duplicate entries on repeated calls.
//
// Caller must hold n.mu.
func (n *Node) persistLog() {
	if err := n.storage.StoreLog(n.log); err != nil {
		n.failStorageWrite("log", err)
	}
}

// persistCommitIndex persists the commit watermark to storage.
//
// Caller must hold n.mu.
func (n *Node) persistCommitIndex() {
	if err := n.storage.SetCommitIndex(n.commitIndex); err != nil {
		n.failStorageWrite("commitIndex", err)
	}
}

// failStorageWrite converts a storage write failure into fail-stop behavior.
//
// Raft safety depends on durable writes completing before responses are sent.
// Continuing after a write failure can violate term/vote/log invariants, so the
// process must crash and restart from durable state.
//
// Caller must hold n.mu.
func (n *Node) failStorageWrite(field string, err error) {
	slog.Error("fatal storage write failure; crashing node process",
		"id", n.id,
		"field", field,
		"err", err,
	)
	panic(fmt.Sprintf("raft: fatal storage write failure (%s): %v", field, err))
}

// Run starts the node's background goroutines.
//
// This method blocks until the context is cancelled.
// It starts:
// - The election timer (for leader election)
// - The apply loop (for applying committed entries)
// - The read sweep (for cleaning up stale pending reads)
func (n *Node) Run(ctx context.Context) {
	// Start the apply loop.
	go n.runApplyLoop(ctx)

	// Start the stale-read sweep so partitioned reads don't accumulate.
	go n.runReadSweep(ctx)

	// Run the election timer (blocks until context is done).
	n.runElectionTimer(ctx)
}

// Kill simulates a node failure.
//
// The node transitions to Dead state and ignores all RPCs.
// Use Restart to bring it back.
func (n *Node) Kill() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Dead {
		return
	}

	slog.Info("node killed", "id", n.id)
	n.state = Dead
	n.notifyStateChange()
}

// Restart brings a dead node back to life by reconstructing from storage.
//
// This simulates process-style crash recovery: volatile state is discarded,
// then persistent state/snapshot are reloaded.
func (n *Node) Restart() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Dead {
		return
	}

	slog.Info("node restarted", "id", n.id)

	n.resetInMemoryStateForRestart()
	n.loadFromStorage()
	n.state = Follower
	n.resetElectionTimer("restart")
	n.notifyStateChange()
}

// resetInMemoryStateForRestart clears volatile and cached state before a
// storage reload. Caller must hold n.mu.
func (n *Node) resetInMemoryStateForRestart() {
	n.currentTerm = 0
	n.votedFor = ""
	n.log = nil

	n.commitIndex = 0
	n.lastApplied = 0
	n.state = Follower
	n.leaderID = ""

	n.nextIndex = nil
	n.matchIndex = nil

	n.snapshotIndex = 0
	n.snapshotTerm = 0
	n.votesReceived = 0
	n.lastHeartbeat = time.Time{}

	n.clusterConfig = nil
	n.pendingSnapshot = nil
	n.clientSessions = make(map[string]ClientSession)
	n.pendingReads = make(map[string]*pendingRead)
	n.heartbeatRound = 0
	n.metrics = newMetrics()

	n.drainSignalChannel(n.electionResetCh)
	n.drainSignalChannel(n.applyReadyCh)
}

// drainSignalChannel removes buffered signal values from channel.
func (n *Node) drainSignalChannel(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// SubmitCommandResult is the return value of SubmitCommand.
//
// It carries both the routing outcome (Accepted / LeaderID) and the
// deduplication outcome (Duplicate / Result) so callers can distinguish
// "not leader" from "already applied".
type SubmitCommandResult struct {
	// Accepted is true when this node is the leader and has appended the entry.
	Accepted bool

	// Duplicate is true when the (ClientID, SequenceNum) pair was already
	// applied.  The cached Result is returned instead of re-executing.
	Duplicate bool

	// LeaderID is the known leader; non-empty when Accepted is false.
	LeaderID string

	// Result is the value returned by the state machine the first time this
	// command was applied.  Only set when Duplicate is true.
	Result interface{}

	// LogIndex is the absolute index where a newly accepted command was
	// appended. Zero when not accepted or duplicate.
	LogIndex int64
}

// SubmitCommand submits a client command to the Raft cluster.
//
// req.ClientID and req.SequenceNum enable exactly-once semantics (§8):
// if the leader has already applied this (ClientID, SequenceNum) pair it
// returns the cached result rather than appending a duplicate entry.
// Both fields may be left zero to disable deduplication.
//
// §5.3: "All client requests are handled by the leader. If a client contacts
// a follower, the follower redirects it to the leader."
func (n *Node) SubmitCommand(req CommandRequest) SubmitCommandResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return SubmitCommandResult{LeaderID: n.leaderID}
	}

	// §8: Return cached result for duplicate requests.
	if dup, result := n.isDuplicate(req.ClientID, req.SequenceNum); dup {
		slog.Debug("duplicate command detected, returning cached result",
			"id", n.id,
			"clientID", req.ClientID,
			"seqNum", req.SequenceNum,
		)
		return SubmitCommandResult{Accepted: true, Duplicate: true, Result: result}
	}

	// Append the command to the log.
	n.log = append(n.log, LogEntry{
		Term:        n.currentTerm,
		Command:     req.Command,
		ClientID:    req.ClientID,
		SequenceNum: req.SequenceNum,
	})

	// §5.3: Persist the log entry before acknowledging.
	n.persistLog()
	n.metrics.incCommandsSubmitted()

	slog.Info("command submitted",
		"id", n.id,
		"index", n.logLen(),
		"command", req.Command,
		"clientID", req.ClientID,
		"seqNum", req.SequenceNum,
	)
	n.notifyStateChange()

	return SubmitCommandResult{Accepted: true, LogIndex: n.logLen()}
}

// Snapshot returns a point-in-time copy of the node's state.
//
// This is safe to call concurrently with other methods.
func (n *Node) Snapshot() StateSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshot()
}

// ID returns the node's unique identifier.
func (n *Node) ID() string {
	return n.id
}

// ── Election Timer ────────────────────────────────────────────────────────────

// runElectionTimer manages the election timeout.
//
// §5.2: "Followers respond to RPCs from candidates and leaders. If a follower
// receives no communication over a period of time called the election timeout,
// it assumes no viable leader exists and begins an election."
func (n *Node) runElectionTimer(ctx context.Context) {
	for {
		// Get a random timeout for this election cycle.
		timeout := n.config.randomElectionTimeout()

		// Remember the term at the start of this cycle.
		n.mu.Lock()
		n.electionTimeout = timeout
		termAtStart := n.currentTerm
		stateAtStart := n.state
		deadline := time.Now().Add(timeout)
		n.mu.Unlock()

		slog.Debug("election timer armed",
			"id", n.id,
			"term", termAtStart,
			"state", stateAtStart,
			"timeout", timeout,
			"deadline", deadline,
		)

		select {
		case <-ctx.Done():
			return

		case <-time.After(timeout):
			firedAt := time.Now()
			// Election timeout fired; verify this cycle is still relevant.
			n.mu.Lock()
			// Skip if term changed (means we already handled this).
			if n.currentTerm != termAtStart {
				slog.Debug("election timeout ignored: term changed",
					"id", n.id,
					"termAtStart", termAtStart,
					"currentTerm", n.currentTerm,
					"firedAt", firedAt,
				)
				n.mu.Unlock()
				continue
			}
			// Leaders don't start elections.
			if n.state == Leader {
				slog.Debug("election timeout ignored: node is leader",
					"id", n.id,
					"term", n.currentTerm,
					"firedAt", firedAt,
				)
				n.mu.Unlock()
				continue
			}
			// Dead nodes don't start elections.
			if n.state == Dead {
				slog.Debug("election timeout ignored: node is dead",
					"id", n.id,
					"term", n.currentTerm,
					"firedAt", firedAt,
				)
				n.mu.Unlock()
				continue
			}

			slog.Debug("election timeout fired",
				"id", n.id,
				"term", n.currentTerm,
				"state", n.state,
				"timeout", timeout,
				"firedAt", firedAt,
			)
			n.mu.Unlock()

			// §3.1: Run Pre-Vote before incrementing term.
			// sendPreVote will call startElection internally if it wins a quorum.
			n.sendPreVote(ctx)

		case <-n.electionResetCh:
			// Timer was reset (by heartbeat or vote). Continue loop.
		}
	}
}

// resetElectionTimer signals that the election timer should reset.
//
// This is called when:
// - The node receives a valid heartbeat from the leader
// - The node grants a vote to a candidate
//
// Caller must hold n.mu.
func (n *Node) resetElectionTimer(reason string) {
	select {
	case n.electionResetCh <- struct{}{}:
		slog.Debug("election timer reset signaled",
			"id", n.id,
			"term", n.currentTerm,
			"state", n.state,
			"reason", reason,
			"leaderID", n.leaderID,
		)
	default:
		slog.Debug("election timer reset coalesced",
			"id", n.id,
			"term", n.currentTerm,
			"state", n.state,
			"reason", reason,
		)
	}
}

// ── Apply Loop ────────────────────────────────────────────────────────────────

// runApplyLoop applies committed log entries to the state machine.
//
// §5.3: "The leader applies log entries to the state machine in log order."
//
// For this visualization, "applying" just means logging that we would apply.
func (n *Node) runApplyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.applyReadyCh:
			n.applyCommittedEntries()
		}
	}
}

// applyCommittedEntries applies all entries up to commitIndex.
//
// Caller does NOT hold the lock.
func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++

		// Convert absolute log index to in-memory slice position.
		memPos := n.lastApplied - n.snapshotIndex - 1
		if memPos < 0 || memPos >= int64(len(n.log)) {
			// Entry has been compacted; it was already applied via snapshot.
			continue
		}
		entry := n.log[memPos]

		// §8: No-op entries are used by the leader to commit previous-term
		// entries indirectly. They must advance lastApplied but must NOT be
		// forwarded to the state machine.
		if entry.Type == LogEntryNoop {
			slog.Debug("noop entry applied (skipped by state machine)",
				"id", n.id,
				"index", n.lastApplied,
				"term", entry.Term,
			)
			continue
		}

		// §6: Config entries update cluster membership when committed.
		// They do not go to the state machine.
		if entry.Type == LogEntryConfig {
			n.applyConfigEntry(entry)
			continue
		}

		// Forward the command to the application state machine.
		var result interface{}
		if n.config.StateMachine != nil {
			result = n.config.StateMachine.Apply(entry.Command)
		}
		n.metrics.incCommandsApplied()

		// §8: Record the session so duplicate retries get the cached result.
		n.recordSession(entry.ClientID, entry.SequenceNum, result)

		slog.Info("entry applied",
			"id", n.id,
			"index", n.lastApplied,
			"term", entry.Term,
			"command", entry.Command,
		)
	}
	n.notifyStateChange()

	// §8: Unblock any linearizable reads whose readIndex has been applied.
	n.signalPendingReads()

	// Trigger an automatic snapshot if the log has grown past the threshold.
	n.maybeSnapshot()
}

// signalApplyReady signals that new entries are ready to be applied.
//
// Caller must hold n.mu.
func (n *Node) signalApplyReady() {
	select {
	case n.applyReadyCh <- struct{}{}:
	default:
		// Already signaled.
	}
}

// ── State Notification ─────────────────────────────────────────────────────────

// notifyStateChange sends the current state to the StateCh channel.
//
// This is non-blocking; if the channel buffer is full, the update is dropped.
// Caller must hold n.mu.
func (n *Node) notifyStateChange() {
	snap := n.snapshot()
	select {
	case n.StateCh <- snap:
	default:
		// Buffer full, drop the update.
	}
}

// snapshot creates a StateSnapshot from the current state.
//
// Caller must hold n.mu.
func (n *Node) snapshot() StateSnapshot {
	// Copy the log to avoid races.
	logCopy := make([]LogEntry, len(n.log))
	copy(logCopy, n.log)

	var nextIndex, matchIndex map[string]int64
	if n.state == Leader {
		nextIndex = make(map[string]int64, len(n.nextIndex))
		for k, v := range n.nextIndex {
			nextIndex[k] = v
		}
		matchIndex = make(map[string]int64, len(n.matchIndex))
		for k, v := range n.matchIndex {
			matchIndex[k] = v
		}
	}

	return StateSnapshot{
		NodeID:              n.id,
		State:               n.state,
		CurrentTerm:         n.currentTerm,
		VotedFor:            n.votedFor,
		CommitIndex:         n.commitIndex,
		LastApplied:         n.lastApplied,
		Log:                 logCopy,
		LeaderID:            n.leaderID,
		SnapshotIndex:       n.snapshotIndex,
		SnapshotTerm:        n.snapshotTerm,
		NextIndex:           nextIndex,
		MatchIndex:          matchIndex,
		HeartbeatIntervalMs: n.config.heartbeatInterval().Milliseconds(),
		ElectionTimeoutMs:   n.snapshotElectionTimeoutMs(),
		Metrics:             n.metrics.snapshot(),
	}
}

// snapshotElectionTimeoutMs returns the active election timeout in
// milliseconds, defaulting to config max when no cycle has been set yet.
//
// Caller must hold n.mu.
func (n *Node) snapshotElectionTimeoutMs() int64 {
	if n.electionTimeout > 0 {
		return n.electionTimeout.Milliseconds()
	}
	return n.config.electionTimeoutMax().Milliseconds()
}

// maybeSnapshot triggers an automatic snapshot in a background goroutine when
// the number of applied entries since the last snapshot exceeds the configured
// threshold.
//
// Caller must hold n.mu.
func (n *Node) maybeSnapshot() {
	threshold := n.config.SnapshotThreshold
	if threshold <= 0 {
		return // Automatic snapshots disabled.
	}
	if n.config.StateMachine == nil || n.config.SnapshotStore == nil {
		return
	}

	appliedSinceSnapshot := n.lastApplied - n.snapshotIndex
	if appliedSinceSnapshot < threshold {
		return
	}

	// Take the snapshot asynchronously so we don't block the apply loop.
	go func() {
		if err := n.TakeSnapshot(); err != nil {
			slog.Error("auto-snapshot failed", "id", n.id, "err", err)
		}
	}()
}

// ── Testing Helpers ────────────────────────────────────────────────────────────

// newTicker is a helper that can be replaced in tests.
// By default, it returns a standard time.Ticker.
func (n *Node) newTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}
