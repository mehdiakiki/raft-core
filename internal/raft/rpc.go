package raft

import (
	"log/slog"
	"time"
)

// RequestVote handles an incoming RequestVote RPC.
//
// §5.2: "Invoked by candidates to gather votes."
//
// A node grants its vote if all of the following are true:
// 1. The candidate's term is >= the node's term
// 2. The node hasn't already voted in this term (or voted for this candidate)
// 3. The candidate's log is at least as up-to-date as the node's log (§5.4.1)
//
// If the candidate's term is higher, the node updates its term and
// steps down to follower (even if it was a leader or candidate).
//
// §5.1: "If RPC request or response contains term T > currentTerm,
// set currentTerm = T and convert to follower."
func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Notify RPC observers of incoming request
	n.rpcObservers.notifyReceive(RpcEvent{
		FromNode:  args.CandidateID,
		ToNode:    n.id,
		RpcType:   "REQUEST_VOTE",
		EventTime: time.Now(),
	})

	// Dead nodes ignore all RPCs.
	if n.state == Dead {
		slog.Debug("RequestVote ignored: node is dead", "id", n.id)
		return RequestVoteReply{}
	}

	reply := RequestVoteReply{Term: n.currentTerm}

	// §5.1: If RPC contains a higher term, update and convert to follower.
	if args.Term > n.currentTerm {
		slog.Info("RequestVote: discovered higher term, stepping down",
			"id", n.id,
			"currentTerm", n.currentTerm,
			"candidateTerm", args.Term,
		)
		n.stepDownToFollower(args.Term)
	}
	if !n.selfIsVoter() {
		slog.Debug("RequestVote: vote denied because receiver is non-voter",
			"id", n.id,
			"term", n.currentTerm,
			"candidate", args.CandidateID,
		)
		reply.Term = n.currentTerm
		n.notifyStateChange()
		return reply
	}

	// §5.2: Grant vote if:
	// 1. Term matches
	// 2. Haven't voted yet OR already voted for this candidate
	// 3. Candidate's log is at least as up-to-date
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	logUpToDate := n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	candidateIsVoter := args.CandidateID == n.id || n.isVoter(args.CandidateID)

	if args.Term == n.currentTerm && canVote && logUpToDate && candidateIsVoter {
		reply.VoteGranted = true
		n.votedFor = args.CandidateID
		n.resetElectionTimer("vote_granted")
		// §5.2: Persist votedFor before responding.
		n.persistVotedFor()
		slog.Info("RequestVote: vote granted",
			"id", n.id,
			"term", n.currentTerm,
			"candidate", args.CandidateID,
		)
	} else {
		slog.Debug("RequestVote: vote denied",
			"id", n.id,
			"term", n.currentTerm,
			"candidate", args.CandidateID,
			"canVote", canVote,
			"logUpToDate", logUpToDate,
			"candidateIsVoter", candidateIsVoter,
		)
	}

	reply.Term = n.currentTerm
	n.notifyStateChange()
	return reply
}

// AppendEntries handles an incoming AppendEntries RPC.
//
// §5.3: "Invoked by leader to replicate log entries; also used as heartbeat."
//
// This method:
// 1. Validates the leader's term
// 2. Checks log consistency using PrevLogIndex and PrevLogTerm
// 3. Appends new entries (truncating conflicting entries)
// 4. Updates commitIndex based on LeaderCommit
//
// §5.1: "If RPC request or response contains term T > currentTerm,
// set currentTerm = T and convert to follower."
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Notify RPC observers of incoming request
	n.rpcObservers.notifyReceive(RpcEvent{
		FromNode:  args.LeaderID,
		ToNode:    n.id,
		RpcType:   "APPEND_ENTRIES",
		EventTime: time.Now(),
	})

	if n.state == Dead {
		slog.Debug("AppendEntries ignored: node is dead", "id", n.id)
		return AppendEntriesReply{}
	}

	reply, ok := n.handleAppendEntriesTermValidation(args)
	if !ok {
		return reply
	}

	n.leaderID = args.LeaderID
	n.resetElectionTimer("append_entries")
	// §3.1: Record when we last heard from a valid leader so PreVote voters
	// can refuse pre-votes while the cluster is healthy.
	n.lastHeartbeat = time.Now()

	reply, ok = n.handleAppendEntriesLogConsistency(args, reply)
	if !ok {
		return reply
	}

	// §5.3: Append new entries to log.
	entriesChanged := n.appendEntries(args.PrevLogIndex, args.Entries)
	if entriesChanged {
		// Persist log entries after appending.
		n.persistLog()
	}

	n.updateCommitIndexAndSignal(args.LeaderCommit)

	reply.Success = true
	reply.Term = n.currentTerm
	slog.Debug("AppendEntries succeeded",
		"id", n.id,
		"leader", args.LeaderID,
		"term", n.currentTerm,
		"entries", len(args.Entries),
	)
	n.notifyStateChange()
	return reply
}

func (n *Node) handleAppendEntriesTermValidation(args AppendEntriesArgs) (AppendEntriesReply, bool) {
	reply := AppendEntriesReply{Term: n.currentTerm, ConflictTerm: -1}

	if args.Term < n.currentTerm {
		slog.Debug("AppendEntries rejected: stale term",
			"id", n.id,
			"currentTerm", n.currentTerm,
			"leaderTerm", args.Term,
		)
		return reply, false
	}

	if args.Term > n.currentTerm {
		slog.Info("AppendEntries: discovered higher term, stepping down",
			"id", n.id,
			"currentTerm", n.currentTerm,
			"leaderTerm", args.Term,
		)
		n.stepDownToFollower(args.Term)
		reply.Term = n.currentTerm
	}

	return reply, true
}

func (n *Node) handleAppendEntriesLogConsistency(args AppendEntriesArgs, reply AppendEntriesReply) (AppendEntriesReply, bool) {
	if args.PrevLogIndex == 0 {
		return reply, true
	}

	// An index that has been compacted into a snapshot is implicitly present.
	if args.PrevLogIndex <= n.snapshotIndex {
		return reply, true
	}

	if args.PrevLogIndex > n.logLen() {
		reply.Success = false
		reply.ConflictIndex = n.logLen() + 1
		reply.ConflictTerm = -1
		slog.Debug("AppendEntries rejected: missing prevLog entry",
			"id", n.id,
			"prevLogIndex", args.PrevLogIndex,
			"logLen", n.logLen(),
		)
		n.notifyStateChange()
		return reply, false
	}

	if n.logTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = n.logTermAt(args.PrevLogIndex)
		// Find the first index in our log with the conflicting term.
		reply.ConflictIndex = args.PrevLogIndex
		for i := n.snapshotIndex + 1; i <= args.PrevLogIndex; i++ {
			if n.logTermAt(i) == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		slog.Debug("AppendEntries rejected: prevLog term mismatch",
			"id", n.id,
			"prevLogIndex", args.PrevLogIndex,
			"expectedTerm", args.PrevLogTerm,
			"actualTerm", n.logTermAt(args.PrevLogIndex),
		)
		n.notifyStateChange()
		return reply, false
	}

	return reply, true
}

func (n *Node) updateCommitIndexAndSignal(leaderCommit int64) {
	if leaderCommit > n.commitIndex {
		n.updateCommitIndex(leaderCommit)
		n.signalApplyReady()
	}
}

// appendEntries appends new entries to the log, truncating any conflicting entries.
// Returns true if the log was modified.
//
// §5.3: "If an existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it."
func (n *Node) appendEntries(prevLogIndex int64, entries []LogEntry) bool {
	if len(entries) == 0 {
		return false // Heartbeat, nothing to append.
	}

	// Convert absolute prevLogIndex to in-memory slice position.
	// insertIndex is the slice position of the first new entry.
	insertIndex := int(prevLogIndex - n.snapshotIndex)
	newEntryIndex := 0

	// Skip entries that already match.
	for insertIndex < len(n.log) && newEntryIndex < len(entries) {
		if n.log[insertIndex].Term != entries[newEntryIndex].Term {
			break // Found a conflict.
		}
		insertIndex++
		newEntryIndex++
	}

	// Truncate conflicting entries and append new ones.
	if newEntryIndex < len(entries) {
		n.log = append(n.log[:insertIndex], entries[newEntryIndex:]...)
		slog.Debug("log entries appended",
			"id", n.id,
			"from", n.snapshotIndex+int64(insertIndex)+1,
			"count", len(entries)-newEntryIndex,
		)
		return true
	}

	return false
}

// updateCommitIndex updates the commit index based on leader's commit index.
//
// §5.3: "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)."
func (n *Node) updateCommitIndex(leaderCommit int64) {
	prevCommitIndex := n.commitIndex
	lastLogIndex := n.logLen()
	if leaderCommit < lastLogIndex {
		n.commitIndex = leaderCommit
	} else {
		n.commitIndex = lastLogIndex
	}
	if n.commitIndex > prevCommitIndex {
		n.persistCommitIndex()
	}
	slog.Debug("commitIndex updated",
		"id", n.id,
		"commitIndex", n.commitIndex,
		"leaderCommit", leaderCommit,
	)
}
