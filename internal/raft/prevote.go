package raft

import (
	"context"
	"log/slog"
	"time"
)

// sendPreVote runs the Pre-Vote phase before a real election (§3.1).
//
// If this node receives pre-vote grants from a quorum of peers, it immediately
// starts a real election.  If not, it stays as follower without incrementing
// its term, preventing unnecessary term inflation when the node is partitioned.
//
// §3.1: "A server does not update its term or grant its vote in the pre-vote,
// so pre-votes cannot delay the progress of the current leader."
func (n *Node) sendPreVote(ctx context.Context) {
	n.mu.Lock()
	if n.state == Dead {
		n.mu.Unlock()
		return
	}
	if !n.selfIsVoter() {
		slog.Debug("pre-vote skipped: node is non-voter",
			"id", n.id,
			"state", n.state,
			"term", n.currentTerm,
		)
		n.mu.Unlock()
		return
	}

	// Pre-vote uses currentTerm+1 so voters can apply the up-to-date check
	// without committing to a new term.
	nextTerm := n.currentTerm + 1
	currentTerm := n.currentTerm
	currentState := n.state
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTerm()
	majority := n.voterCount()/2 + 1
	peerIDs := n.voterIDs()
	slog.Debug("pre-vote started",
		"id", n.id,
		"currentTerm", currentTerm,
		"nextTerm", nextTerm,
		"state", currentState,
		"peers", len(peerIDs),
		"majority", majority,
		"lastLogIndex", lastLogIndex,
		"lastLogTerm", lastLogTerm,
	)
	n.mu.Unlock()

	grants := n.collectPreVoteGrants(ctx, peerIDs, nextTerm, lastLogIndex, lastLogTerm, majority)
	if grants < majority {
		slog.Debug("pre-vote failed",
			"id", n.id,
			"currentTerm", currentTerm,
			"nextTerm", nextTerm,
			"grants", grants,
			"majority", majority,
		)
		return
	}

	slog.Debug("pre-vote succeeded, starting real election",
		"id", n.id,
		"currentTerm", currentTerm,
		"nextTerm", nextTerm,
		"grants", grants,
		"majority", majority,
	)
	n.startElection(ctx)
}

// collectPreVoteGrants fans out PreVote RPCs to all peers and returns the
// total number of grants (including the self-vote which is always counted).
//
// Returns as soon as the result is determined (quorum or all replies received).
func (n *Node) collectPreVoteGrants(
	ctx context.Context,
	peerIDs []string,
	nextTerm, lastLogIndex, lastLogTerm int64,
	majority int,
) int {
	// Buffered so goroutines never block after the caller returns early.
	results := make(chan bool, len(peerIDs))

	for _, peerID := range peerIDs {
		go func(pid string) {
			granted := n.sendPreVoteTo(ctx, pid, nextTerm, lastLogIndex, lastLogTerm)
			results <- granted
		}(peerID)
	}

	// Self always grants its own pre-vote; start count at 1.
	count := 1
	for range peerIDs {
		if <-results {
			count++
		}
		if count >= majority {
			break
		}
	}
	return count
}

// sendPreVoteTo sends a single PreVote RPC to the named peer and returns
// whether the vote was granted.  Steps down if a higher term is observed.
func (n *Node) sendPreVoteTo(ctx context.Context, peerID string, nextTerm, lastLogIndex, lastLogTerm int64) bool {
	n.rpcObservers.notifySend(RpcEvent{
		FromNode:    n.id,
		ToNode:      peerID,
		RpcType:     "PRE_VOTE",
		RpcID:       preVoteRPCID(nextTerm, n.id, peerID),
		EventTime:   time.Now(),
		Term:        nextTerm,
		HasTerm:     true,
		CandidateID: n.id,
	})

	peer := n.peers[peerID]
	reply, err := peer.PreVote(ctx, PreVoteArgs{
		NextTerm:     nextTerm,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		slog.Debug("PreVote RPC failed", "id", n.id, "peer", peerID, "error", err)
		return false
	}

	n.rpcObservers.notifyReceive(RpcEvent{
		FromNode:    peerID,
		ToNode:      n.id,
		RpcType:     "PRE_VOTE_REPLY",
		RpcID:       preVoteReplyRPCID(nextTerm, peerID, n.id),
		EventTime:   time.Now(),
		Term:        nextTerm,
		HasTerm:     true,
		CandidateID: n.id,
		VoteGranted: boolPtr(reply.VoteGranted),
	})

	// §5.1: Step down if a higher term is observed.
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		slog.Info("pre-vote: discovered higher term, stepping down",
			"id", n.id, "replyTerm", reply.Term)
		n.stepDownToFollower(reply.Term)
		return false
	}
	return reply.VoteGranted
}

// PreVote handles an incoming PreVote RPC (§3.1).
//
// A pre-vote is granted when both conditions hold:
//  1. The candidate's log is at least as up-to-date as ours (§5.4.1).
//  2. We have NOT heard from a valid leader within the election timeout window,
//     meaning our own election timer could reasonably fire soon.
func (n *Node) PreVote(args PreVoteArgs) PreVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.rpcObservers.notifyReceive(RpcEvent{
		FromNode:    args.CandidateID,
		ToNode:      n.id,
		RpcType:     "PRE_VOTE",
		RpcID:       preVoteRPCID(args.NextTerm, args.CandidateID, n.id),
		EventTime:   time.Now(),
		Term:        args.NextTerm,
		HasTerm:     true,
		CandidateID: args.CandidateID,
	})

	emitPreVoteReply := func(reply PreVoteReply) {
		n.rpcObservers.notifySend(RpcEvent{
			FromNode:    n.id,
			ToNode:      args.CandidateID,
			RpcType:     "PRE_VOTE_REPLY",
			RpcID:       preVoteReplyRPCID(args.NextTerm, n.id, args.CandidateID),
			EventTime:   time.Now(),
			Term:        args.NextTerm,
			HasTerm:     true,
			CandidateID: args.CandidateID,
			VoteGranted: boolPtr(reply.VoteGranted),
		})
	}

	if n.state == Dead {
		reply := PreVoteReply{}
		emitPreVoteReply(reply)
		return reply
	}

	reply := PreVoteReply{Term: n.currentTerm}
	if !n.selfIsVoter() {
		slog.Debug("PreVote: denied because receiver is non-voter",
			"id", n.id, "candidate", args.CandidateID)
		emitPreVoteReply(reply)
		return reply
	}

	// Condition 1: candidate log up-to-date (same criterion as real vote).
	logUpToDate := n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	candidateIsVoter := args.CandidateID == n.id || n.isVoter(args.CandidateID)

	// Condition 2: our election timer has not been recently reset by a leader.
	// We use twice the minimum election timeout as a freshness window.
	leaderFresh := n.leaderIsRecentlyHeard()

	if logUpToDate && candidateIsVoter && !leaderFresh {
		reply.VoteGranted = true
		slog.Debug("PreVote: granted",
			"id", n.id, "candidate", args.CandidateID, "nextTerm", args.NextTerm)
	} else {
		slog.Debug("PreVote: denied",
			"id", n.id, "candidate", args.CandidateID,
			"logUpToDate", logUpToDate, "candidateIsVoter", candidateIsVoter, "leaderFresh", leaderFresh)
	}

	emitPreVoteReply(reply)
	return reply
}

// leaderIsRecentlyHeard reports whether the node has received a valid
// AppendEntries within the freshness window (2 × min election timeout).
//
// Caller must hold n.mu.
func (n *Node) leaderIsRecentlyHeard() bool {
	if n.lastHeartbeat.IsZero() {
		return false
	}
	freshness := n.config.electionTimeoutMin() * 2
	return time.Since(n.lastHeartbeat) < freshness
}
