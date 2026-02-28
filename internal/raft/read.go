package raft

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// ReadIndex implements reads at a configurable consistency level (§8).
//
// Linearizable (default):
//  1. Reject if not leader.
//  2. Record readIndex = commitIndex.
//  3. Send a heartbeat round; wait for quorum acknowledgement.
//  4. Wait until lastApplied >= readIndex.
//  5. Return readIndex to the caller.
//
// LeaderLocal (lower latency):
//
//	Skip the heartbeat; assume this node is still the leader.
//	Wait until lastApplied >= commitIndex, then return.
//	Callers may observe stale data if this node was recently deposed.
//
// Any (stale read):
//
//	Return the caller's lastApplied immediately.
//	No ordering guarantees; may be served by any node.
//
// The read ID must be unique per request; an empty readID is rejected.
func (n *Node) ReadIndex(ctx context.Context, readID string, consistency ConsistencyLevel) ReadIndexResult {
	if readID == "" {
		return ReadIndexResult{}
	}
	n.metrics.incReadIndexRequests()

	switch consistency {
	case Any:
		return n.readIndexAny()
	case LeaderLocal:
		return n.readIndexLeaderLocal(ctx, readID)
	default:
		return n.readIndexLinearizable(ctx, readID)
	}
}

// readIndexLinearizable is the §8 full linearizable path.
func (n *Node) readIndexLinearizable(ctx context.Context, readID string) ReadIndexResult {
	n.mu.Lock()

	if n.state != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return ReadIndexResult{LeaderID: hint}
	}

	pr := n.registerPendingRead(ctx, readID)
	term := n.currentTerm
	n.mu.Unlock()

	// Trigger a heartbeat round so followers can ack us as current leader.
	n.sendHeartbeats(term)

	// Wait for quorum confirmation or context cancellation.
	select {
	case <-ctx.Done():
		n.cancelPendingRead(readID)
		return ReadIndexResult{LeaderID: n.leaderID}
	case <-pr.leadershipConfirmed:
	}

	// Wait for state machine to catch up to readIndex.
	if err := n.waitForApply(ctx, pr.readIndex); err != nil {
		n.cancelPendingRead(readID)
		return ReadIndexResult{LeaderID: n.leaderID}
	}

	slog.Debug("read index confirmed",
		"id", n.id,
		"readID", readID,
		"readIndex", pr.readIndex,
	)
	return ReadIndexResult{Success: true, ReadIndex: pr.readIndex}
}

// readIndexLeaderLocal skips the heartbeat round.
//
// This is safe in stable clusters where the caller accepts the small risk of
// reading from a just-deposed leader.
func (n *Node) readIndexLeaderLocal(ctx context.Context, readID string) ReadIndexResult {
	n.mu.Lock()
	if n.state != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return ReadIndexResult{LeaderID: hint}
	}
	target := n.commitIndex
	n.mu.Unlock()

	if err := n.waitForApply(ctx, target); err != nil {
		return ReadIndexResult{LeaderID: n.leaderID}
	}

	slog.Debug("leader-local read index confirmed",
		"id", n.id,
		"readID", readID,
		"readIndex", target,
	)
	return ReadIndexResult{Success: true, ReadIndex: target}
}

// readIndexAny returns immediately with the current lastApplied index.
//
// No ordering guarantees; the caller may observe stale data.  Useful for
// read-heavy workloads where approximate freshness is acceptable.
func (n *Node) readIndexAny() ReadIndexResult {
	n.mu.Lock()
	idx := n.lastApplied
	n.mu.Unlock()
	return ReadIndexResult{Success: true, ReadIndex: idx}
}

// registerPendingRead creates and stores a pendingRead for the given readID.
//
// Caller must hold n.mu.
func (n *Node) registerPendingRead(ctx context.Context, readID string) *pendingRead {
	// Majority is calculated over Voters only so NonVoter learners don't
	// inflate the quorum requirement for read confirmations.
	majority := n.voterCount()/2 + 1
	requiredRound := n.heartbeatRound + 1
	pr := &pendingRead{
		ctx:                 ctx,
		readIndex:           n.commitIndex,
		requiredRound:       requiredRound,
		quorumCount:         1, // Count self.
		ackedPeers:          map[string]struct{}{n.id: {}},
		majority:            majority,
		leadershipConfirmed: make(chan struct{}),
	}
	// Single-node cluster: self alone is a majority.
	if pr.quorumCount >= pr.majority {
		close(pr.leadershipConfirmed)
	}
	n.pendingReads[readID] = pr
	return pr
}

// advancePendingReads records one peer heartbeat acknowledgement for every
// in-flight read and closes leadershipConfirmed for reads that reach quorum.
//
// Called from handleReplicationSuccess; caller must hold n.mu.
func (n *Node) advancePendingReads(peerID string, heartbeatRound uint64) {
	if peerID == "" {
		return
	}

	for id, pr := range n.pendingReads {
		select {
		case <-pr.leadershipConfirmed:
			// Already confirmed; clean up if also applied.
			if n.lastApplied >= pr.readIndex {
				delete(n.pendingReads, id)
			}
			continue
		default:
		}

		if heartbeatRound < pr.requiredRound {
			continue
		}
		if _, seen := pr.ackedPeers[peerID]; seen {
			continue
		}
		pr.ackedPeers[peerID] = struct{}{}
		pr.quorumCount++
		if pr.quorumCount >= pr.majority {
			close(pr.leadershipConfirmed)
		}
	}
}

// signalPendingReads removes entries whose readIndex has been applied.
//
// Called from applyCommittedEntries after lastApplied advances; caller must
// hold n.mu.
func (n *Node) signalPendingReads() {
	for id, pr := range n.pendingReads {
		select {
		case <-pr.leadershipConfirmed:
			if n.lastApplied >= pr.readIndex {
				delete(n.pendingReads, id)
			}
		default:
		}
	}
}

// cancelPendingRead removes a pending read that timed out or was cancelled.
func (n *Node) cancelPendingRead(readID string) {
	n.mu.Lock()
	delete(n.pendingReads, readID)
	n.mu.Unlock()
}

// sweepStalePendingReads removes every pendingRead whose context has already
// been cancelled (or whose leadershipConfirmed channel was never closed).
//
// This prevents unbounded accumulation of pendingRead entries when the node
// is partitioned and no quorum can form for an extended period.
//
// Caller must hold n.mu.
func (n *Node) sweepStalePendingReads() {
	for id, pr := range n.pendingReads {
		select {
		case <-pr.leadershipConfirmed:
			// Already confirmed but not yet applied — keep it so waitForApply
			// can finish normally.
		default:
			// Leadership not yet confirmed.  If the associated context is done
			// the waiting goroutine has already (or will soon) call
			// cancelPendingRead; remove it now to avoid a leak.
			if pr.ctx.Err() != nil {
				delete(n.pendingReads, id)
			}
		}
	}
}

// runReadSweep periodically sweeps stale pending reads.
//
// Started as a background goroutine by Run.
func (n *Node) runReadSweep(ctx context.Context) {
	ticker := time.NewTicker(n.config.heartbeatInterval() * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.done:
			return
		case <-ticker.C:
			n.mu.Lock()
			n.sweepStalePendingReads()
			n.mu.Unlock()
		}
	}
}

// waitForApply blocks until lastApplied >= target or ctx is cancelled.
//
// Uses a short poll rather than consuming from StateCh so that concurrent
// readers do not starve each other.
func (n *Node) waitForApply(ctx context.Context, target int64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= target {
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for apply: %w", ctx.Err())
		case <-time.After(2 * time.Millisecond):
			// Poll again.
		}
	}
}
