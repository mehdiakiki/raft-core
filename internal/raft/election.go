package raft

import (
	"context"
	"log/slog"
	"time"
)

// startElection begins a new election term.
//
// §5.2: "To begin an election, a follower increments its current term and
// transitions to candidate state. It then votes for itself and issues
// RequestVote RPCs in parallel to each of the other servers in the cluster."
//
// A candidate wins the election if it receives votes from a majority of servers.
// The election may result in:
// 1. Winning the election (becoming leader)
// 2. Losing the election (another node becomes leader)
// 3. Split vote (no winner, new election after timeout)
func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	if !n.selfIsVoter() {
		slog.Debug("election skipped: node is non-voter",
			"id", n.id,
			"term", n.currentTerm,
		)
		n.mu.Unlock()
		return
	}

	// Transition to candidate and increment term.
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id   // Vote for self.
	n.votesReceived = 1 // Count our own vote.
	n.metrics.incElectionsStarted()

	// §5.1: Persist term and votedFor before requesting votes.
	n.persistTerm()
	n.persistVotedFor()

	// Capture values for the goroutines.
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTerm()

	// Get list of voter peer IDs (NonVoters do not participate in elections).
	peerIDs := n.voterIDs()

	slog.Info("election started",
		"id", n.id,
		"term", term,
		"peers", len(peerIDs),
	)
	n.notifyStateChange()
	n.mu.Unlock()

	// Request votes from all peers in parallel.
	for _, peerID := range peerIDs {
		go func(pid string) {
			// Notify observers of outgoing RPC
			n.rpcObservers.notifySend(RpcEvent{
				FromNode:  n.id,
				ToNode:    pid,
				RpcType:   "REQUEST_VOTE",
				EventTime: time.Now(),
			})

			peer := n.peers[pid]
			reply, err := peer.RequestVote(ctx, RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				slog.Debug("RequestVote RPC failed",
					"id", n.id,
					"peer", pid,
					"error", err,
				)
				return
			}

			n.processVoteReply(pid, term, reply)
		}(peerID)
	}
}

// processVoteReply handles a vote response from a peer.
//
// This is called asynchronously for each RequestVote response.
// The candidate wins if it receives votes from a majority.
func (n *Node) processVoteReply(peerID string, electionTerm int64, reply RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// §5.1: A higher term means we are outdated. Step down.
	if reply.Term > n.currentTerm {
		slog.Info("discovered higher term in vote reply, stepping down",
			"id", n.id,
			"currentTerm", n.currentTerm,
			"replyTerm", reply.Term,
		)
		n.stepDownToFollower(reply.Term)
		return
	}

	// Ignore if we're no longer a candidate or term changed.
	if n.state != Candidate || n.currentTerm != electionTerm {
		return
	}

	// Count the vote if granted.
	if reply.VoteGranted {
		if !n.isVoter(peerID) {
			slog.Debug("ignoring vote from non-voter peer",
				"id", n.id,
				"term", electionTerm,
				"peer", peerID,
			)
			return
		}
		n.votesReceived++
		slog.Debug("vote received",
			"id", n.id,
			"term", electionTerm,
			"from", peerID,
			"total", n.votesReceived,
		)

		// Majority is calculated over Voters only (NonVoters cannot vote).
		majority := n.voterCount()/2 + 1
		if n.votesReceived >= majority {
			n.becomeLeader()
		}
	}
}

// becomeLeader transitions the node to leader state.
//
// §5.2: "Once a candidate wins an election, it becomes leader. It then sends
// heartbeat messages to all of the other servers to establish its authority
// and prevent new elections."
//
// §8: A new leader appends a no-op entry in its own term immediately on
// election. This causes any uncommitted entries from previous terms to be
// committed indirectly (they precede the no-op), preventing the cluster from
// stalling if the leader never receives a client command.
//
// Caller must hold n.mu.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.metrics.incElectionsWon()

	// §8: Append a no-op entry in the new leader's term.
	// This is the canonical way to commit entries from previous terms:
	// once this no-op is committed, all preceding entries are committed too.
	noopEntry := LogEntry{Term: n.currentTerm, Type: LogEntryNoop}
	n.log = append(n.log, noopEntry)
	n.persistLog()

	// Initialize leader volatile state AFTER appending the no-op.
	// nextIndex[peer] = len(log): peers don't have the no-op yet, so
	// replication starts at the no-op entry, delivering it on the first
	// heartbeat/AppendEntries.
	n.nextIndex = make(map[string]int64, len(n.peers))
	n.matchIndex = make(map[string]int64, len(n.peers))
	for peerID := range n.peers {
		n.nextIndex[peerID] = n.logLen() // send noop on first replication
		n.matchIndex[peerID] = 0
	}

	slog.Info("became leader",
		"id", n.id,
		"term", n.currentTerm,
		"noopIndex", len(n.log),
	)
	n.notifyStateChange()

	// Start a term-scoped heartbeat goroutine.
	// Passing the term at the moment of election ensures that if this node
	// steps down and a new heartbeat loop is started in a later term, the
	// old goroutine will detect the term mismatch and exit cleanly.
	go n.runHeartbeatLoop(n.currentTerm)
}

// stepDownToFollower transitions the node to follower state.
//
// This is called when:
// - Discovering a higher term from another node
// - Receiving a valid AppendEntries from a leader
//
// Persists term and votedFor before returning so a crash cannot leave
// inconsistent state (e.g. old votedFor from a previous term surviving
// across a restart into the new, higher term).
//
// Caller must hold n.mu.
func (n *Node) stepDownToFollower(newTerm int64) {
	wasLeader := n.state == Leader
	n.state = Follower
	n.currentTerm = newTerm
	n.votedFor = ""
	n.resetElectionTimer("step_down")

	// §5.1: Persist both term and cleared votedFor atomically.
	// votedFor MUST be persisted here: if we crash after persisting term
	// but before persisting votedFor="", on restart we would load the new
	// term with a stale votedFor from the previous term, violating the
	// "vote at most once per term" invariant.
	n.persistTerm()
	n.persistVotedFor()

	if wasLeader {
		slog.Info("stepped down from leader to follower",
			"id", n.id,
			"newTerm", newTerm,
		)
	}
}

// runHeartbeatLoop sends periodic heartbeats to all peers.
//
// §5.2: "The leader sends periodic heartbeats (AppendEntries RPCs that carry
// no log entries) to all followers to maintain its authority."
//
// leaderTerm is the term at the moment this node became leader. The loop exits
// as soon as the node's current term advances past leaderTerm (step-down) or
// the node is no longer leader — whichever comes first. This prevents goroutine
// leaks on rapid leader-to-follower-to-leader cycles.
func (n *Node) runHeartbeatLoop(leaderTerm int64) {
	interval := n.config.heartbeatInterval()
	ticker := n.newTicker(interval)
	defer ticker.Stop()

	for {
		// Check if we're still the leader in the same term.
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != leaderTerm {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		n.mu.Unlock()

		// Send heartbeats to all peers.
		n.sendHeartbeats(term)

		// Wait for next heartbeat interval.
		select {
		case <-ticker.C:
			continue
		case <-n.done:
			return
		}
	}
}

// sendHeartbeats sends AppendEntries RPCs to all peers.
//
// Each heartbeat is sent in a separate goroutine for parallelism.
// The heartbeat may also include log entries if the peer is behind.
func (n *Node) sendHeartbeats(term int64) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	n.heartbeatRound++
	round := n.heartbeatRound

	// Copy peers map to avoid holding lock during RPCs.
	peers := make(map[string]Peer, len(n.peers))
	for id, peer := range n.peers {
		peers[id] = peer
	}
	n.mu.Unlock()

	// Send to each peer in parallel.
	for peerID, peer := range peers {
		go n.replicateToPeer(peerID, peer, term, round)
	}
}
