package raft

import "context"

// Peer is the interface for communicating with another Raft node.
//
// This interface abstracts the transport layer (gRPC in our case),
// making the core Raft logic independent of the communication mechanism.
// This also makes unit testing straightforward with mock implementations.
type Peer interface {
	// RequestVote sends a RequestVote RPC to the peer (§5.2).
	RequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error)

	// AppendEntries sends an AppendEntries RPC to the peer (§5.3).
	AppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error)

	// PreVote sends a PreVote RPC to the peer (§3.1).
	// A pre-vote lets a candidate check that it can win an election before
	// incrementing its term, preventing partitioned nodes from disrupting
	// the cluster when they rejoin.
	PreVote(ctx context.Context, args PreVoteArgs) (PreVoteReply, error)
}

// LogEntryType describes the kind of a log entry.
//
// Having an explicit type allows the apply loop and state machine to distinguish
// no-op entries (used for leader commitment) from real client commands without
// relying on sentinel values like an empty command string.
type LogEntryType int

const (
	// LogEntryCommand is a normal client command to be applied to the state machine.
	LogEntryCommand LogEntryType = iota

	// LogEntryNoop is a no-op entry appended by a new leader immediately on
	// election to commit any entries from previous terms (§8).
	LogEntryNoop

	// LogEntryConfig carries a serialized ClusterConfig for a membership change.
	// §6: Membership changes are replicated through the log so they go through
	// consensus before taking effect.
	LogEntryConfig
)

// LogEntry represents a single entry in the replicated log.
//
// §5.3: "Each entry contains a term number (used to detect inconsistencies)
// and a command (for the state machine)."
type LogEntry struct {
	// Term is the term when this entry was created.
	// Used for log matching and leader election.
	Term int64

	// Type is the kind of this log entry (command or no-op).
	// The apply loop uses this to skip no-op entries.
	Type LogEntryType

	// Command is the client request to be applied to the state machine.
	// In a key-value store, this might be "SET x=5".
	// Empty when Type == LogEntryNoop.
	Command string

	// ClientID is the unique identifier of the originating client.
	// Empty for internal entries (no-ops).
	// Together with SequenceNum, it enables exactly-once semantics (§8).
	ClientID string

	// SequenceNum is the per-client monotonically increasing command counter.
	// Zero means deduplication is disabled for this entry.
	SequenceNum int64

	// ConfigData is the JSON-encoded ClusterConfig for LogEntryConfig entries.
	// Empty for all other entry types.
	ConfigData []byte
}

// ClientSession records the last command applied for a given client.
//
// §8: "Each client assigns unique serial numbers to every command. The
// state machine tracks the latest serial number processed for each client,
// along with the associated response. If it receives a command whose serial
// number has already been executed, it responds immediately without re-executing."
type ClientSession struct {
	// LastSeqNum is the sequence number of the most recently applied command.
	LastSeqNum int64

	// LastResult is the value returned by the state machine for LastSeqNum.
	// Cached so duplicate requests receive the same response.
	LastResult interface{}
}

// RequestVoteArgs contains the arguments for a RequestVote RPC.
//
// §5.2, Figure 2: "Invoked by candidates to gather votes."
type RequestVoteArgs struct {
	// Term is the candidate's current term.
	Term int64

	// CandidateID is the ID of the candidate requesting the vote.
	CandidateID string

	// LastLogIndex is the index of the candidate's last log entry.
	// Used to determine if the candidate's log is at least as up-to-date.
	LastLogIndex int64

	// LastLogTerm is the term of the candidate's last log entry.
	// Used with LastLogIndex for the up-to-date check (§5.4.1).
	LastLogTerm int64
}

// RequestVoteReply contains the response to a RequestVote RPC.
//
// §5.2, Figure 2: "Response to RequestVote RPC."
type RequestVoteReply struct {
	// Term is the current term of the responder.
	// Used by the candidate to update its term if outdated.
	Term int64

	// VoteGranted is true if the candidate received the vote.
	VoteGranted bool
}

// PreVoteArgs contains the arguments for a PreVote RPC.
//
// §3.1: Before incrementing its term, a candidate sends a PreVote to check
// that it can win an election.  The term field is the term the candidate
// *would* use (currentTerm + 1), so voters can apply the same up-to-date
// check without committing to a term change.
type PreVoteArgs struct {
	// NextTerm is the term the candidate would use if it starts a real election.
	NextTerm int64

	// CandidateID is the ID of the candidate requesting the pre-vote.
	CandidateID string

	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex int64

	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm int64
}

// PreVoteReply contains the response to a PreVote RPC.
type PreVoteReply struct {
	// Term is the current term of the voter.
	Term int64

	// VoteGranted is true if the voter would grant a real vote in NextTerm.
	VoteGranted bool
}

// AppendEntriesArgs contains the arguments for an AppendEntries RPC.
//
// §5.3, Figure 2: "Invoked by leader to replicate log entries; also used as heartbeat."
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term int64

	// LeaderID is the ID of the leader.
	// Used by followers to redirect clients.
	LeaderID string

	// PrevLogIndex is the index of the log entry immediately preceding
	// the new entries. Used for log consistency check.
	PrevLogIndex int64

	// PrevLogTerm is the term of the log entry at PrevLogIndex.
	// Used for log consistency check.
	PrevLogTerm int64

	// Entries are the log entries to store.
	// Empty for heartbeat RPCs.
	Entries []LogEntry

	// LeaderCommit is the leader's commitIndex.
	// Followers update their commitIndex accordingly.
	LeaderCommit int64
}

// AppendEntriesReply contains the response to an AppendEntries RPC.
//
// §5.3, Figure 2: "Response to AppendEntries RPC."
type AppendEntriesReply struct {
	// Term is the current term of the responder.
	// Used by the leader to update its term if outdated.
	Term int64

	// Success is true if the follower contained an entry matching
	// PrevLogIndex and PrevLogTerm.
	Success bool

	// ConflictIndex is the first index of the conflicting term.
	// Used for fast log backtracking optimization (§5.3, last paragraph).
	// Set to 0 if not applicable.
	ConflictIndex int64

	// ConflictTerm is the term of the conflicting entry.
	// Set to -1 if there was no entry at PrevLogIndex.
	ConflictTerm int64
}

// StateSnapshot is a point-in-time copy of a node's observable state.
//
// This is sent to the gateway via the StateCh channel for visualization.
// All fields are copies to avoid race conditions.
type StateSnapshot struct {
	// NodeID is the unique identifier of this node.
	NodeID string

	// State is the current role (Follower, Candidate, Leader, or Dead).
	State NodeState

	// CurrentTerm is the latest term this node has seen.
	CurrentTerm int64

	// VotedFor is the candidate ID this node voted for in the current term.
	// Empty string if no vote has been cast.
	VotedFor string

	// CommitIndex is the index of the highest log entry known to be committed.
	CommitIndex int64

	// LastApplied is the index of the highest log entry applied to the state machine.
	LastApplied int64

	// Log is a copy of the current in-memory log entries (post-compaction).
	Log []LogEntry

	// LeaderID is the ID of the current known leader.
	// Empty string if leader is unknown.
	LeaderID string

	// SnapshotIndex is the index of the last log entry covered by a snapshot.
	// 0 means no snapshot has been taken.
	SnapshotIndex int64

	// SnapshotTerm is the term of the last log entry covered by a snapshot.
	SnapshotTerm int64

	// NextIndex is the leader's next-index map (§5.3, Figure 2).
	// nil for non-leaders.
	NextIndex map[string]int64

	// MatchIndex is the leader's match-index map (§5.3, Figure 2).
	// nil for non-leaders.
	MatchIndex map[string]int64

	// HeartbeatIntervalMs is the configured leader heartbeat interval in
	// milliseconds (before any frontend visualization scaling).
	HeartbeatIntervalMs int64

	// ElectionTimeoutMs is this node's current election timeout for the active
	// timer cycle, in milliseconds (before any frontend visualization scaling).
	ElectionTimeoutMs int64

	// Metrics is a snapshot of protocol counters.
	Metrics MetricsSnapshot
}

// CommandRequest carries a client command and the optional deduplication fields.
//
// Setting ClientID and SequenceNum enables exactly-once semantics (§8).
// Leave both zero to submit a fire-and-forget command without deduplication.
type CommandRequest struct {
	// Command is the payload to be applied to the state machine.
	Command string

	// ClientID uniquely identifies the originating client.
	// Empty disables deduplication for this request.
	ClientID string

	// SequenceNum is the client's per-request monotonic counter.
	// Zero disables deduplication for this request.
	SequenceNum int64
}

// ConsistencyLevel controls the consistency guarantee for ReadIndex requests.
//
// §8 of the Raft paper defines only the linearizable path; the additional
// levels are practical extensions that trade consistency for latency.
type ConsistencyLevel int

const (
	// Linearizable requires a quorum heartbeat round before the read is
	// allowed to proceed.  This is the default and matches §8 exactly:
	// the leader proves it has not been superseded before serving the read.
	Linearizable ConsistencyLevel = iota

	// LeaderLocal skips the heartbeat round and relies on the leader's
	// in-memory commit index.  Reads may be served from a deposed leader
	// during a brief window after a partition heals, so this is not
	// strictly linearizable.  It offers lower latency in stable clusters.
	LeaderLocal

	// Any allows the read to proceed immediately on any node, using only
	// that node's lastApplied index.  No ordering guarantees are provided;
	// the caller may observe arbitrarily stale data.
	Any
)

// ReadIndexResult is the return value of ReadIndex.
//
// On success ReadIndex contains the log index up to which the state machine
// is consistent for a linearizable read.  On failure (not leader) LeaderID
// contains a forwarding hint.
type ReadIndexResult struct {
	// ReadIndex is the commitIndex captured when the read was initiated.
	// The caller must wait until lastApplied >= ReadIndex before querying
	// the state machine to guarantee linearizability (§8).
	ReadIndex int64

	// LeaderID is the known leader; non-empty when Success is false.
	LeaderID string

	// Success is true when this node confirmed its leadership and the
	// ReadIndex was established.
	Success bool
}

// pendingRead tracks a single in-flight linearizable read request (§8).
//
// The read can proceed once:
//  1. A quorum of peers has acknowledged a heartbeat sent after the read
//     was registered (proving this node is still the current leader), AND
//  2. lastApplied has caught up to ReadIndex.
//
// Fields are written once under n.mu and read by the waiting goroutine.
type pendingRead struct {
	// ctx is the caller's context; used by the sweep goroutine to detect
	// abandoned reads and clean them up before the next heartbeat round.
	ctx context.Context

	// readIndex is the commitIndex at the moment the read was registered.
	readIndex int64

	// requiredRound is the minimum heartbeat round that can satisfy this read.
	// Acks from older rounds are ignored.
	requiredRound uint64

	// quorumCount is the number of peers that have acked the heartbeat.
	// Starts at 1 (self).  Under n.mu.
	quorumCount int

	// ackedPeers tracks unique peers that have acked at/after requiredRound.
	// Prevents duplicate acks from one peer from falsely satisfying quorum.
	ackedPeers map[string]struct{}

	// majority is the count needed to confirm leadership.
	majority int

	// leadershipConfirmed is closed when quorumCount >= majority.
	leadershipConfirmed chan struct{}
}
