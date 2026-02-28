package raft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
)

// Snapshot-related constants.
const (
	// SnapshotThreshold is the default number of applied log entries that
	// triggers an automatic snapshot.  A value of 0 disables auto-snapshots.
	SnapshotThreshold = 100

	// snapshotChunkSize is the maximum number of bytes sent in a single
	// InstallSnapshot RPC.  Keeping chunks small avoids hitting gRPC's default
	// 4 MiB message size limit on large state machines.
	snapshotChunkSize = 512 * 1024 // 512 KiB
)

// SnapshotMeta contains the metadata stored alongside a snapshot.
//
// §7: A snapshot replaces all log entries up to and including LastIndex.
// After a snapshot is installed the log is compacted to free memory.
type SnapshotMeta struct {
	// LastIndex is the index of the last log entry reflected in the snapshot.
	LastIndex int64

	// LastTerm is the term of the last log entry reflected in the snapshot.
	LastTerm int64

	// Configuration is the cluster membership at the time of the snapshot.
	// Restoring it prevents a split-brain after a snapshot install on a follower
	// that has never seen the current config via log replication.
	// nil for snapshots taken before membership tracking was added.
	Configuration *ClusterConfig
}

// SnapshotStore provides durable snapshot storage.
//
// The Raft node uses this to:
//   - Save a snapshot and the corresponding metadata after log compaction.
//   - Load the latest snapshot on startup or when installing a snapshot from
//     the leader.
//
// Implementations must be safe for concurrent use.
type SnapshotStore interface {
	// Save atomically persists a snapshot together with its metadata.
	Save(meta SnapshotMeta, data []byte) error

	// Load returns the most recent snapshot and its metadata.
	// Returns ErrNoSnapshot if no snapshot has been saved yet.
	Load() (SnapshotMeta, []byte, error)
}

// ErrNoSnapshot is returned by SnapshotStore.Load when no snapshot exists.
var ErrNoSnapshot = errors.New("raft: no snapshot available")

// pendingSnapshotState accumulates chunked snapshot data received from the leader.
//
// The leader may split a large snapshot into multiple InstallSnapshot RPCs
// (one chunk per call).  We buffer the data here until the final chunk
// (Done == true) arrives, then atomically install the snapshot.
type pendingSnapshotState struct {
	// meta holds the index/term/config sent in the first chunk (Offset == 0).
	meta SnapshotMeta

	// buf accumulates received bytes in order.
	buf *bytes.Buffer
}

// InstallSnapshotArgs contains the arguments for an InstallSnapshot RPC.
//
// §7: The leader uses InstallSnapshot to send snapshots to followers that
// are too far behind.  Large snapshots are split into multiple chunks so
// individual RPCs stay within gRPC message size limits.
type InstallSnapshotArgs struct {
	// Term is the leader's current term.
	Term int64

	// LeaderID is the ID of the leader sending the snapshot.
	LeaderID string

	// LastIncludedIndex is the index of the last log entry in the snapshot.
	LastIncludedIndex int64

	// LastIncludedTerm is the term of the last log entry in the snapshot.
	LastIncludedTerm int64

	// Configuration is the cluster config at LastIncludedIndex.
	// Sent only in the first chunk (Offset == 0); ignored in later chunks.
	Configuration *ClusterConfig

	// Offset is the byte offset of Data within the full snapshot payload.
	// The first chunk always has Offset == 0.
	Offset int64

	// Data is the chunk payload for this RPC.
	Data []byte

	// Done is true when this is the final chunk of the snapshot.
	Done bool
}

// InstallSnapshotReply contains the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	// Term is the responder's current term.
	// Used by the leader to step down if its term is stale.
	Term int64
}

// SnapshotPeer extends Peer with the ability to send InstallSnapshot RPCs.
type SnapshotPeer interface {
	Peer

	// InstallSnapshot sends an InstallSnapshot RPC to the peer (§7).
	InstallSnapshot(ctx context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// TakeSnapshot captures the current state machine state and compacts the log.
//
// §7: "The leader periodically snapshots its state and discards the log
// entries that precede the snapshot."
//
// TakeSnapshot is safe to call from any goroutine.  It acquires the node
// lock briefly to capture metadata, releases it while the state machine
// serialises, then re-acquires it to compact the log.
//
// Returns an error if no StateMachine or SnapshotStore is configured.
func (n *Node) TakeSnapshot() error {
	n.mu.Lock()
	if n.config.StateMachine == nil {
		n.mu.Unlock()
		return errors.New("raft: no state machine configured")
	}
	if n.config.SnapshotStore == nil {
		n.mu.Unlock()
		return errors.New("raft: no snapshot store configured")
	}

	// Capture the point-in-time index/term for this snapshot.
	snapIndex := n.lastApplied
	snapTerm := n.logTermAt(snapIndex)
	if snapTerm == 0 && snapIndex > 0 {
		// Fall back to cached snapshot term if the entry has already been compacted.
		snapTerm = n.snapshotTerm
	}

	// Capture a copy of the current cluster config for the snapshot.
	var cfgCopy *ClusterConfig
	if n.clusterConfig != nil {
		members := make([]ClusterMember, len(n.clusterConfig.Members))
		copy(members, n.clusterConfig.Members)
		cfgCopy = &ClusterConfig{Members: members}
	}
	n.mu.Unlock()

	// Serialise the state machine outside the lock to avoid blocking RPCs.
	rc, err := n.config.StateMachine.Snapshot()
	if err != nil {
		return err
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return err
	}

	// Persist the snapshot (with cluster config).
	meta := SnapshotMeta{
		LastIndex:     snapIndex,
		LastTerm:      snapTerm,
		Configuration: cfgCopy,
	}
	if err := n.config.SnapshotStore.Save(meta, data); err != nil {
		return err
	}

	// Compact the in-memory log: discard entries up to snapIndex.
	n.mu.Lock()
	n.compactLogUpTo(snapIndex, snapTerm)
	n.mu.Unlock()

	slog.Info("snapshot taken",
		"id", n.id,
		"lastIndex", snapIndex,
		"lastTerm", snapTerm,
		"dataBytes", len(data),
	)
	return nil
}

// compactLogUpTo discards in-memory log entries with absolute index <= upTo.
//
// After compaction:
//   - n.snapshotIndex = upTo
//   - n.snapshotTerm  = term
//   - n.log contains only entries with index > upTo
//
// Caller must hold n.mu.
func (n *Node) compactLogUpTo(upTo int64, term int64) {
	if upTo <= n.snapshotIndex {
		return // Nothing to compact.
	}

	// Number of entries to discard from the head of the in-memory log.
	trimCount := int(upTo - n.snapshotIndex)
	if trimCount > len(n.log) {
		trimCount = len(n.log)
	}

	n.log = n.log[trimCount:]
	n.snapshotIndex = upTo
	n.snapshotTerm = term

	slog.Debug("log compacted",
		"id", n.id,
		"snapshotIndex", n.snapshotIndex,
		"remainingEntries", len(n.log),
	)
}

// InstallSnapshot handles an incoming InstallSnapshot RPC from the leader.
//
// §7: Large snapshots are split into chunks by the leader; we accumulate them
// in a pendingSnapshot buffer and install atomically on the last chunk (Done).
func (n *Node) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Dead {
		return InstallSnapshotReply{}
	}

	reply := InstallSnapshotReply{Term: n.currentTerm}

	// §5.1: Reject if sender's term is stale.
	if args.Term < n.currentTerm {
		return reply
	}

	// §5.1: Higher term implies we're outdated; step down.
	if args.Term > n.currentTerm {
		n.stepDownToFollower(args.Term)
		reply.Term = n.currentTerm
	}

	n.leaderID = args.LeaderID
	n.resetElectionTimer("install_snapshot")

	// First chunk: initialise the pending buffer.
	if args.Offset == 0 {
		// Idempotent: if we already have this snapshot (or newer), ignore.
		if args.LastIncludedIndex <= n.snapshotIndex {
			return reply
		}
		n.pendingSnapshot = &pendingSnapshotState{
			meta: SnapshotMeta{
				LastIndex:     args.LastIncludedIndex,
				LastTerm:      args.LastIncludedTerm,
				Configuration: args.Configuration,
			},
			buf: &bytes.Buffer{},
		}
	}

	// Guard: if we have no pending buffer we can't process this chunk.
	if n.pendingSnapshot == nil {
		return reply
	}

	// Verify the chunk is in order.
	if args.Offset != int64(n.pendingSnapshot.buf.Len()) {
		// Offset mismatch — discard the partial transfer; leader will retry.
		n.pendingSnapshot = nil
		return reply
	}

	n.pendingSnapshot.buf.Write(args.Data)

	if !args.Done {
		// More chunks to come.
		return reply
	}

	// Final chunk: install the snapshot.
	n.finalizeSnapshotInstall()
	return reply
}

// finalizeSnapshotInstall atomically installs the buffered snapshot.
//
// Caller must hold n.mu.
func (n *Node) finalizeSnapshotInstall() {
	pending := n.pendingSnapshot
	n.pendingSnapshot = nil

	data := pending.buf.Bytes()

	// Restore state machine if one is configured.
	if n.config.StateMachine != nil {
		rc := io.NopCloser(bytes.NewReader(data))
		if err := n.config.StateMachine.Restore(rc); err != nil {
			slog.Error("InstallSnapshot: state machine restore failed",
				"id", n.id, "err", err)
			return
		}
	}

	// Persist the snapshot if a store is configured.
	if n.config.SnapshotStore != nil {
		if err := n.config.SnapshotStore.Save(pending.meta, data); err != nil {
			slog.Error("InstallSnapshot: persist failed", "id", n.id, "err", err)
			// Continue — in-memory state is still updated.
		}
	}

	// Discard the log and advance volatile state.
	n.log = nil
	n.snapshotIndex = pending.meta.LastIndex
	n.snapshotTerm = pending.meta.LastTerm

	if pending.meta.LastIndex > n.commitIndex {
		n.commitIndex = pending.meta.LastIndex
		n.persistCommitIndex()
	}
	if pending.meta.LastIndex > n.lastApplied {
		n.lastApplied = pending.meta.LastIndex
	}

	// Restore cluster config from the snapshot.
	if pending.meta.Configuration != nil {
		n.clusterConfig = pending.meta.Configuration
	}

	n.notifyStateChange()

	slog.Info("snapshot installed",
		"id", n.id,
		"lastIndex", pending.meta.LastIndex,
		"lastTerm", pending.meta.LastTerm,
	)
}

// sendSnapshotToPeer sends the current snapshot to a peer that is too far
// behind for normal AppendEntries to catch it up.
//
// The snapshot is streamed in chunks of snapshotChunkSize bytes so we never
// exceed gRPC's message size limit, even for large state machines.
//
// Called by the replication loop when nextIndex[peer] <= snapshotIndex.
func (n *Node) sendSnapshotToPeer(peerID string, peer SnapshotPeer, leaderTerm int64) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != leaderTerm {
		n.mu.Unlock()
		return
	}
	if n.config.SnapshotStore == nil {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderID := n.id

	// Copy config pointer while holding the lock.
	var cfgCopy *ClusterConfig
	if n.clusterConfig != nil {
		members := make([]ClusterMember, len(n.clusterConfig.Members))
		copy(members, n.clusterConfig.Members)
		cfgCopy = &ClusterConfig{Members: members}
	}
	n.mu.Unlock()

	meta, data, err := n.config.SnapshotStore.Load()
	if err != nil {
		slog.Debug("sendSnapshotToPeer: no snapshot available",
			"id", n.id, "peer", peerID, "err", err)
		return
	}

	n.streamSnapshot(peerID, peer, term, leaderID, meta, cfgCopy, data)
}

// streamSnapshot sends snapshot data to peer in sequential chunks.
func (n *Node) streamSnapshot(
	peerID string,
	peer SnapshotPeer,
	term int64,
	leaderID string,
	meta SnapshotMeta,
	cfg *ClusterConfig,
	data []byte,
) {
	total := int64(len(data))
	var offset int64

	for {
		end := offset + snapshotChunkSize
		if end > total {
			end = total
		}
		chunk := data[offset:end]
		done := end >= total

		args := InstallSnapshotArgs{
			Term:              term,
			LeaderID:          leaderID,
			LastIncludedIndex: meta.LastIndex,
			LastIncludedTerm:  meta.LastTerm,
			Offset:            offset,
			Data:              chunk,
			Done:              done,
		}
		// Configuration is only sent once in the first chunk.
		if offset == 0 {
			args.Configuration = cfg
		}

		reply, err := peer.InstallSnapshot(context.Background(), args)
		if err != nil {
			slog.Debug("InstallSnapshot RPC failed",
				"id", n.id, "peer", peerID, "err", err)
			return
		}

		n.mu.Lock()
		if reply.Term > n.currentTerm {
			n.stepDownToFollower(reply.Term)
			n.mu.Unlock()
			return
		}
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if done {
			// Advance nextIndex past the snapshot so normal replication resumes.
			if meta.LastIndex+1 > n.nextIndex[peerID] {
				n.nextIndex[peerID] = meta.LastIndex + 1
			}
			if meta.LastIndex > n.matchIndex[peerID] {
				n.matchIndex[peerID] = meta.LastIndex
			}
			n.mu.Unlock()

			slog.Info("snapshot sent to peer",
				"id", n.id,
				"peer", peerID,
				"lastIndex", meta.LastIndex,
				"chunks", offset/snapshotChunkSize+1,
			)
			return
		}
		n.mu.Unlock()

		offset = end
	}
}
