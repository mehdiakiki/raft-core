package raft

import (
	"context"
	"log/slog"
	"time"
)

// replicateToPeer sends AppendEntries RPCs to a single peer.
//
// This method handles:
// 1. Determining which entries to send (based on nextIndex)
// 2. Sending the AppendEntries RPC
// 3. Processing the response (success or conflict)
// 4. Updating nextIndex and matchIndex on success
// 5. Advancing commitIndex when a majority have replicated
//
// §5.3: "The leader maintains a nextIndex for each follower, which is the
// index of the next log entry the leader will send to that follower."
func (n *Node) replicateToPeer(peerID string, peer Peer, leaderTerm int64, heartbeatRound uint64) {
	n.mu.Lock()

	// Verify we're still the leader.
	if n.state != Leader || n.currentTerm != leaderTerm {
		n.mu.Unlock()
		return
	}

	nextIndex := n.nextIndex[peerID]

	// §7: If the follower is so far behind that we have already compacted
	// the entries it needs, send a snapshot instead.
	if nextIndex <= n.snapshotIndex {
		snapshotPeer, ok := peer.(SnapshotPeer)
		n.mu.Unlock()
		if ok {
			n.sendSnapshotToPeer(peerID, snapshotPeer, leaderTerm)
		} else {
			slog.Warn("peer does not support InstallSnapshot; cannot catch up lagging follower",
				"id", n.id, "peer", peerID)
		}
		return
	}

	// Get the next entry to send to this peer.
	prevLogIndex := nextIndex - 1
	prevLogTerm := n.logTermAt(prevLogIndex)

	// Get entries to send (from nextIndex to end of log).
	entries := n.logSliceFrom(nextIndex)

	// Get current commitIndex.
	leaderCommit := n.commitIndex

	n.mu.Unlock()

	// Notify observers of outgoing RPC
	n.rpcObservers.notifySend(RpcEvent{
		FromNode:  n.id,
		ToNode:    peerID,
		RpcType:   "APPEND_ENTRIES",
		RpcID:     appendEntriesRPCID(leaderTerm, n.id, peerID, heartbeatRound),
		EventTime: time.Now(),
		Term:      leaderTerm,
		HasTerm:   true,
	})

	// Send the AppendEntries RPC.
	reply, err := peer.AppendEntries(context.Background(), AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})

	if err != nil {
		slog.Debug("AppendEntries RPC failed",
			"id", n.id,
			"peer", peerID,
			"error", err,
		)
		return
	}

	n.processAppendEntriesReply(peerID, leaderTerm, heartbeatRound, prevLogIndex, entries, reply)
}

// processAppendEntriesReply handles the response from an AppendEntries RPC.
//
// On success: updates nextIndex and matchIndex, and may advance commitIndex.
// On failure: decrements nextIndex (with fast backtracking optimization).
func (n *Node) processAppendEntriesReply(
	peerID string,
	leaderTerm int64,
	heartbeatRound uint64,
	prevLogIndex int64,
	entries []LogEntry,
	reply AppendEntriesReply,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// §5.1: Higher term means we're outdated. Step down.
	if reply.Term > n.currentTerm {
		slog.Info("discovered higher term in AppendEntries reply, stepping down",
			"id", n.id,
			"currentTerm", n.currentTerm,
			"replyTerm", reply.Term,
		)
		n.stepDownToFollower(reply.Term)
		return
	}

	// Ignore if we're no longer leader or term changed.
	if n.state != Leader || n.currentTerm != leaderTerm {
		return
	}

	if reply.Success {
		n.handleReplicationSuccess(peerID, heartbeatRound, prevLogIndex, entries)
	} else {
		n.handleReplicationFailure(peerID, reply)
	}
}

// handleReplicationSuccess updates state after successful replication.
//
// §5.3: "If successful, update nextIndex and matchIndex for the follower."
func (n *Node) handleReplicationSuccess(peerID string, heartbeatRound uint64, prevLogIndex int64, entries []LogEntry) {
	newMatchIndex := prevLogIndex + int64(len(entries))

	// Only update if we made progress.
	if newMatchIndex > n.matchIndex[peerID] {
		delta := newMatchIndex - n.matchIndex[peerID]
		n.matchIndex[peerID] = newMatchIndex
		n.nextIndex[peerID] = newMatchIndex + 1
		n.metrics.addLogEntriesReplicated(delta)

		slog.Debug("replication successful",
			"id", n.id,
			"peer", peerID,
			"matchIndex", newMatchIndex,
		)

		// Try to advance commitIndex.
		n.advanceCommitIndex()
	}

	// §8: Every successful AppendEntries ack (including heartbeats) counts as
	// evidence that this node is still the current leader.  Advance pending
	// linearizable reads regardless of whether matchIndex changed.
	n.advancePendingReads(peerID, heartbeatRound)

	// §6: Check if this peer is a NonVoter that has now caught up and
	// should be promoted to Voter.
	n.maybePromoteNonVoter(peerID)
} // handleReplicationFailure handles a failed AppendEntries.
// §5.3: "If AppendEntries fails because of log inconsistency, decrement
// nextIndex and retry."
//
// We use the fast backtracking optimization described in §5.3's last paragraph.
func (n *Node) handleReplicationFailure(peerID string, reply AppendEntriesReply) {
	var newNextIndex int64

	// Fast backtracking: use conflict hints.
	if reply.ConflictTerm > 0 {
		// Find the last entry in our log with the conflicting term.
		lastIndexForTerm := int64(-1)
		for i := int64(len(n.log)) - 1; i >= 0; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				lastIndexForTerm = i + 1
				break
			}
		}

		if lastIndexForTerm > 0 {
			// Skip to after the last entry with that term.
			newNextIndex = lastIndexForTerm + 1
		} else {
			// We don't have that term, use their conflict index.
			newNextIndex = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		// No conflict term, use conflict index.
		newNextIndex = reply.ConflictIndex
	} else {
		// No hints provided, decrement by 1.
		newNextIndex = n.nextIndex[peerID] - 1
	}

	// Ensure nextIndex doesn't go below 1.
	if newNextIndex < 1 {
		newNextIndex = 1
	}

	n.nextIndex[peerID] = newNextIndex

	slog.Debug("replication failed, adjusted nextIndex",
		"id", n.id,
		"peer", peerID,
		"nextIndex", newNextIndex,
		"conflictIndex", reply.ConflictIndex,
		"conflictTerm", reply.ConflictTerm,
	)
}

// advanceCommitIndex advances commitIndex if a majority of Voters have replicated.
//
// §5.3: "If there exists an N such that N > commitIndex, a majority of
// matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N."
//
// Only entries from the current term can be committed directly (Figure 8).
// NonVoters are excluded from the quorum count so learners cannot block commits.
func (n *Node) advanceCommitIndex() {
	// Try each index from highest to current commitIndex.
	for N := n.logLen(); N > n.commitIndex; N-- {
		// §5.4.2: Only commit entries from current term (Figure 8 safety).
		if n.logTermAt(N) != n.currentTerm {
			continue
		}

		// Count how many Voters have this entry (self always counts).
		count := 1
		for peerID, matchIdx := range n.matchIndex {
			if matchIdx >= N && n.isVoter(peerID) {
				count++
			}
		}

		// Majority is calculated over all Voters (including self).
		majority := n.voterCount()/2 + 1
		if count >= majority {
			n.commitIndex = N
			n.persistCommitIndex()
			n.signalApplyReady()
			n.notifyStateChange()

			slog.Info("commitIndex advanced",
				"id", n.id,
				"commitIndex", N,
				"replicas", count,
			)
			break
		}
	}
}
