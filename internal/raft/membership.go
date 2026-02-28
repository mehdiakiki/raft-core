package raft

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// MemberRole distinguishes full voting members from non-voting learners.
//
// §6: A new server is added as a NonVoter (learner) first.  It replicates
// the log but does not count toward quorum.  Once it catches up it is
// automatically promoted to Voter so it does not slow down commits while
// lagging behind.
type MemberRole int

const (
	// Voter is a full cluster member that participates in elections and
	// counts toward commit quorum.
	Voter MemberRole = iota

	// NonVoter is a learner that receives log replication but cannot vote
	// and is excluded from quorum calculations.
	NonVoter
)

// ClusterMember describes a single participant in the Raft cluster.
type ClusterMember struct {
	// ID is the unique node identifier.
	ID string `json:"id"`

	// Role is Voter or NonVoter.
	Role MemberRole `json:"role"`
}

// ClusterConfig is the authoritative membership list at a point in time.
//
// It is embedded in SnapshotMeta so membership survives snapshot installs,
// and it is stored as ConfigData in LogEntryConfig entries so changes are
// replicated through consensus.
type ClusterConfig struct {
	// Members is the ordered list of all cluster participants.
	Members []ClusterMember `json:"members"`
}

// encodeConfig serialises a ClusterConfig to JSON.
func encodeConfig(cfg *ClusterConfig) ([]byte, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("raft: encode cluster config: %w", err)
	}
	return data, nil
}

// decodeConfig deserialises a ClusterConfig from JSON.
func decodeConfig(data []byte) (*ClusterConfig, error) {
	var cfg ClusterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("raft: decode cluster config: %w", err)
	}
	return &cfg, nil
}

// voterIDs returns the IDs of all Voter members other than this node.
//
// Caller must hold n.mu.
func (n *Node) voterIDs() []string {
	if n.clusterConfig == nil {
		// No explicit config: treat all peers as voters.
		ids := make([]string, 0, len(n.peers))
		for id := range n.peers {
			ids = append(ids, id)
		}
		return ids
	}

	voters := make([]string, 0, len(n.clusterConfig.Members))
	for _, m := range n.clusterConfig.Members {
		if m.ID != n.id && m.Role == Voter {
			voters = append(voters, m.ID)
		}
	}
	return voters
}

// isVoter reports whether the given peer ID is a full Voter.
//
// Returns true when there is no explicit config (backwards-compatible).
// Caller must hold n.mu.
func (n *Node) isVoter(peerID string) bool {
	if n.clusterConfig == nil {
		return true
	}
	for _, m := range n.clusterConfig.Members {
		if m.ID == peerID {
			return m.Role == Voter
		}
	}
	return false
}

// selfIsVoter reports whether this node is currently a voting member.
//
// When there is no explicit clusterConfig, all nodes are treated as voters.
// If a config exists but does not contain self, fail closed and return false.
//
// Caller must hold n.mu.
func (n *Node) selfIsVoter() bool {
	if n.clusterConfig == nil {
		return true
	}
	for _, m := range n.clusterConfig.Members {
		if m.ID == n.id {
			return m.Role == Voter
		}
	}
	return false
}

// voterCount returns the total number of Voters in the cluster (including self).
//
// Caller must hold n.mu.
func (n *Node) voterCount() int {
	if n.clusterConfig == nil {
		return len(n.peers) + 1
	}
	count := 0
	for _, m := range n.clusterConfig.Members {
		if m.Role == Voter {
			count++
		}
	}
	return count
}

// AddPeer adds a new member to the cluster as a NonVoter (learner).
//
// §6: The new peer starts as NonVoter so it can replicate without affecting
// quorum.  It will be auto-promoted to Voter once it catches up
// (matchIndex >= commitIndex).
//
// A LogEntryConfig entry is appended to the log so the change goes through
// consensus and is replicated to all nodes before taking effect.
//
// Returns an error if this node is not the leader or the peer already exists.
func (n *Node) AddPeer(id string, peer Peer) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return &NotLeaderError{LeaderID: n.leaderID}
	}
	if _, exists := n.peers[id]; exists {
		return fmt.Errorf("raft: peer %q already exists", id)
	}
	if id == n.id {
		return fmt.Errorf("raft: cannot add self as peer")
	}

	// Register the peer in the transport layer immediately so it starts
	// receiving AppendEntries (it needs the log to be able to catch up).
	n.peers[id] = peer
	n.nextIndex[id] = n.logLen() + 1
	n.matchIndex[id] = 0

	// Build the new config with the peer as NonVoter.
	newCfg := n.buildNewConfig(id, NonVoter, false)
	if err := n.appendConfigEntry(newCfg); err != nil {
		// Roll back the in-memory peer registration on config failure.
		delete(n.peers, id)
		delete(n.nextIndex, id)
		delete(n.matchIndex, id)
		return err
	}

	slog.Info("peer added as NonVoter",
		"id", n.id,
		"peer", id,
	)
	return nil
}

// RemovePeer removes a member from the cluster.
//
// A LogEntryConfig entry is appended to the log so the change is replicated
// through consensus.
//
// Returns an error if this node is not the leader or the peer does not exist.
func (n *Node) RemovePeer(id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return &NotLeaderError{LeaderID: n.leaderID}
	}
	if _, exists := n.peers[id]; !exists {
		return fmt.Errorf("raft: peer %q not found", id)
	}

	newCfg := n.buildNewConfig(id, Voter, true /* remove */)
	if err := n.appendConfigEntry(newCfg); err != nil {
		return err
	}

	// Remove from transport immediately; replication will cease.
	delete(n.peers, id)
	delete(n.nextIndex, id)
	delete(n.matchIndex, id)

	slog.Info("peer removed",
		"id", n.id,
		"peer", id,
	)
	return nil
}

// Peers returns a copy of the current peer map.
//
// Safe to call concurrently; the returned map is a snapshot of the peer
// set at the time of the call.
func (n *Node) Peers() map[string]Peer {
	n.mu.Lock()
	defer n.mu.Unlock()

	result := make(map[string]Peer, len(n.peers))
	for id, peer := range n.peers {
		result[id] = peer
	}
	return result
}

// buildNewConfig derives a new ClusterConfig from the current one.
//
// If remove is true the member with the given id is excluded.
// Otherwise the member is added (or updated) with the given role.
//
// Caller must hold n.mu.
func (n *Node) buildNewConfig(id string, role MemberRole, remove bool) *ClusterConfig {
	// Start from current config or synthesise one from the in-memory peer set.
	var base []ClusterMember
	if n.clusterConfig != nil {
		base = make([]ClusterMember, len(n.clusterConfig.Members))
		copy(base, n.clusterConfig.Members)
	} else {
		// Bootstrap: self + all current peers as Voters.
		base = append(base, ClusterMember{ID: n.id, Role: Voter})
		for pid := range n.peers {
			base = append(base, ClusterMember{ID: pid, Role: Voter})
		}
	}

	if remove {
		filtered := base[:0]
		for _, m := range base {
			if m.ID != id {
				filtered = append(filtered, m)
			}
		}
		return &ClusterConfig{Members: filtered}
	}

	// Update existing member or append new one.
	found := false
	for i, m := range base {
		if m.ID == id {
			base[i].Role = role
			found = true
			break
		}
	}
	if !found {
		base = append(base, ClusterMember{ID: id, Role: role})
	}
	return &ClusterConfig{Members: base}
}

// appendConfigEntry encodes cfg and appends a LogEntryConfig log entry.
//
// Caller must hold n.mu.
func (n *Node) appendConfigEntry(cfg *ClusterConfig) error {
	data, err := encodeConfig(cfg)
	if err != nil {
		return err
	}
	n.log = append(n.log, LogEntry{
		Term:       n.currentTerm,
		Type:       LogEntryConfig,
		ConfigData: data,
	})
	n.persistLog()
	return nil
}

// applyConfigEntry applies a committed LogEntryConfig to in-memory membership.
//
// This is called from applyCommittedEntries.  Caller must hold n.mu.
func (n *Node) applyConfigEntry(entry LogEntry) {
	cfg, err := decodeConfig(entry.ConfigData)
	if err != nil {
		slog.Error("failed to decode config entry",
			"id", n.id,
			"err", err,
		)
		return
	}

	n.clusterConfig = cfg
	slog.Info("cluster config applied",
		"id", n.id,
		"members", len(cfg.Members),
	)
}

// reconstructConfigFromLog scans committed in-memory log entries in reverse and
// restores clusterConfig from the most recent LogEntryConfig entry.
//
// This is called on startup (loadFromStorage) so a restarted node has the
// correct membership without waiting for the first committed log entry to
// pass through the apply loop.
//
// Caller must not hold n.mu.
func (n *Node) reconstructConfigFromLog() {
	lastCommitted := n.commitIndex
	if lastCommitted > n.logLen() {
		lastCommitted = n.logLen()
	}
	if lastCommitted <= n.snapshotIndex {
		return
	}

	lastCommittedPos := int(lastCommitted - n.snapshotIndex - 1)
	if lastCommittedPos >= len(n.log) {
		lastCommittedPos = len(n.log) - 1
	}

	for i := lastCommittedPos; i >= 0; i-- {
		if n.log[i].Type != LogEntryConfig {
			continue
		}
		cfg, err := decodeConfig(n.log[i].ConfigData)
		if err != nil {
			slog.Error("reconstructConfigFromLog: decode failed",
				"id", n.id, "err", err)
			return
		}
		n.clusterConfig = cfg
		slog.Info("cluster config reconstructed from log",
			"id", n.id,
			"members", len(cfg.Members),
		)
		return
	}
}

// maybePromoteNonVoter checks whether a peer has caught up and, if so,
// promotes it from NonVoter to Voter by appending a new config entry.
//
// Promotion happens when matchIndex[peer] >= commitIndex, meaning the peer
// holds all committed entries and can safely participate in quorum.
//
// Caller must hold n.mu.
func (n *Node) maybePromoteNonVoter(peerID string) {
	if n.state != Leader || n.clusterConfig == nil {
		return
	}

	// Only promote NonVoters.
	for _, m := range n.clusterConfig.Members {
		if m.ID != peerID || m.Role != NonVoter {
			continue
		}
		if n.matchIndex[peerID] < n.commitIndex {
			return
		}
		// Caught up — promote to Voter.
		newCfg := n.buildNewConfig(peerID, Voter, false)
		if err := n.appendConfigEntry(newCfg); err != nil {
			slog.Error("failed to append promotion config entry",
				"id", n.id,
				"peer", peerID,
				"err", err,
			)
			return
		}
		slog.Info("NonVoter promoted to Voter",
			"id", n.id,
			"peer", peerID,
		)
		return
	}
}

// NotLeaderError is returned when a write operation is sent to a non-leader.
//
// The LeaderID field provides a redirect hint for clients.
type NotLeaderError struct {
	// LeaderID is the known leader; may be empty if leader is unknown.
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not the leader (leader unknown)"
	}
	return fmt.Sprintf("not the leader (current leader: %s)", e.LeaderID)
}
