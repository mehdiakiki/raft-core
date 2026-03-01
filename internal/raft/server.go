package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	pb "github.com/mehdiakiki/raft-core/gen/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const submitCommitWaitTimeout = 2 * time.Second

// Server wraps a Node and exposes it as a pb.RaftServiceServer.
// It translates between the protobuf wire types and the pure-Go types used
// by the core algorithm.
type Server struct {
	pb.UnimplementedRaftServiceServer
	node *Node
}

// NewServer creates a gRPC server backed by the given Node.
func NewServer(node *Node) *Server {
	return &Server{node: node}
}

// ── Peer-to-peer RPCs ────────────────────────────────────────────────────────

// RequestVote handles a RequestVote RPC from a candidate peer (§5.2).
func (s *Server) RequestVote(_ context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	reply := s.node.RequestVote(RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &pb.RequestVoteReply{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

// AppendEntries handles an AppendEntries RPC from the current leader (§5.3).
func (s *Server) AppendEntries(_ context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	entries := make([]LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = LogEntry{
			Term:        e.Term,
			Type:        protoTypeToLogEntryType(e.Type),
			Command:     e.Command,
			ClientID:    e.ClientId,
			SequenceNum: e.SequenceNum,
			ConfigData:  e.ConfigData,
		}
	}
	reply := s.node.AppendEntries(AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

// ── Gateway observability RPCs ───────────────────────────────────────────────

// GetState returns the current observable state of this node.
func (s *Server) GetState(_ context.Context, _ *pb.GetStateRequest) (*pb.NodeStateReply, error) {
	snap := s.node.Snapshot()
	return snapshotToProto(snap), nil
}

// WatchState streams state updates to the caller until the context is cancelled.
// The gateway calls this once per node to receive a continuous state feed.
func (s *Server) WatchState(_ *pb.WatchStateRequest, stream pb.RaftService_WatchStateServer) error {
	// Send the current state immediately so the gateway has an initial value.
	snap := s.node.Snapshot()
	if err := stream.Send(&pb.NodeStateUpdate{State: snapshotToProto(snap)}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case snap, ok := <-s.node.StateCh:
			if !ok {
				return io.EOF
			}
			if err := stream.Send(&pb.NodeStateUpdate{State: snapshotToProto(snap)}); err != nil {
				return err
			}
		}
	}
}

// ── Gateway command RPCs ─────────────────────────────────────────────────────

// SubmitCommand forwards a client command to the Raft log (leader only).
func (s *Server) SubmitCommand(ctx context.Context, req *pb.SubmitCommandRequest) (*pb.SubmitCommandReply, error) {
	result := s.node.SubmitCommand(CommandRequest{
		Command:     req.Command,
		ClientID:    req.ClientId,
		SequenceNum: req.SequenceNum,
	})
	reply := &pb.SubmitCommandReply{
		Success:   result.Accepted,
		LeaderId:  result.LeaderID,
		Duplicate: result.Duplicate,
	}
	if !result.Accepted {
		return reply, nil
	}

	if result.Duplicate {
		reply.Committed = true
		reply.Result = stringifyResult(result.Result)
		return reply, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, submitCommitWaitTimeout)
	defer cancel()
	if err := s.node.waitForApply(waitCtx, result.LogIndex); err != nil {
		slog.Warn("submit command not yet committed before timeout",
			"node", s.node.ID(),
			"logIndex", result.LogIndex,
			"clientID", req.ClientId,
			"seqNum", req.SequenceNum,
			"err", err,
		)
		return reply, nil
	}

	reply.Committed = true
	if cached, ok := s.node.sessionResultForExactSeq(req.ClientId, req.SequenceNum); ok {
		reply.Result = stringifyResult(cached)
	}
	return reply, nil
}

// ReadIndex handles a linearizable read request (§8).
//
// The handler confirms leadership via a heartbeat quorum, waits until
// lastApplied >= commitIndex, then returns the safe read index so the caller
// can query the state machine without stale data.
func (s *Server) ReadIndex(ctx context.Context, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error) {
	result := s.node.ReadIndex(ctx, req.ReadId, protoToConsistencyLevel(req.Consistency))
	if !result.Success {
		return &pb.ReadIndexReply{Success: false, LeaderId: result.LeaderID}, nil
	}
	return &pb.ReadIndexReply{Success: true, ReadIndex: result.ReadIndex}, nil
}

// protoToConsistencyLevel maps a protobuf ConsistencyLevel enum to the Go type.
func protoToConsistencyLevel(c pb.ConsistencyLevel) ConsistencyLevel {
	switch c {
	case pb.ConsistencyLevel_LEADER_LOCAL:
		return LeaderLocal
	case pb.ConsistencyLevel_ANY:
		return Any
	default:
		return Linearizable
	}
}

// PreVote handles a PreVote RPC from a candidate peer (§3.1).
//
// A voter grants the pre-vote if it would grant a real vote AND has not heard
// from a valid leader recently (i.e. its election timer has not been reset).
func (s *Server) PreVote(_ context.Context, req *pb.PreVoteArgs) (*pb.PreVoteReply, error) {
	reply := s.node.PreVote(PreVoteArgs{
		NextTerm:     req.NextTerm,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &pb.PreVoteReply{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

// SetAlive kills or restarts the node, simulating node failure/recovery.
func (s *Server) SetAlive(_ context.Context, req *pb.SetAliveRequest) (*pb.SetAliveReply, error) {
	if req.Alive {
		s.node.Restart()
	} else {
		s.node.Kill()
	}
	snap := s.node.Snapshot()
	return &pb.SetAliveReply{Alive: snap.State != Dead}, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func snapshotToProto(snap StateSnapshot) *pb.NodeStateReply {
	entries := make([]*pb.LogEntry, len(snap.Log))
	for i, e := range snap.Log {
		entries[i] = &pb.LogEntry{
			Term:        e.Term,
			Type:        logEntryTypeToProto(e.Type),
			Command:     e.Command,
			ClientId:    e.ClientID,
			SequenceNum: e.SequenceNum,
			ConfigData:  e.ConfigData,
		}
	}
	return &pb.NodeStateReply{
		NodeId:              snap.NodeID,
		State:               snap.State.String(),
		CurrentTerm:         snap.CurrentTerm,
		VotedFor:            snap.VotedFor,
		CommitIndex:         snap.CommitIndex,
		LastApplied:         snap.LastApplied,
		Log:                 entries,
		LeaderId:            snap.LeaderID,
		NextIndex:           snap.NextIndex,
		MatchIndex:          snap.MatchIndex,
		HeartbeatIntervalMs: snap.HeartbeatIntervalMs,
		ElectionTimeoutMs:   snap.ElectionTimeoutMs,
		Metrics: &pb.ProtocolMetrics{
			ElectionsStarted:     snap.Metrics.ElectionsStarted,
			ElectionsWon:         snap.Metrics.ElectionsWon,
			CommandsSubmitted:    snap.Metrics.CommandsSubmitted,
			CommandsApplied:      snap.Metrics.CommandsApplied,
			LogEntriesReplicated: snap.Metrics.LogEntriesReplicated,
			ReadIndexRequests:    snap.Metrics.ReadIndexRequests,
		},
	}
}

// logEntryTypeToProto converts a raft.LogEntryType to its protobuf representation.
func logEntryTypeToProto(t LogEntryType) pb.LogEntryType {
	switch t {
	case LogEntryNoop:
		return pb.LogEntryType_LOG_ENTRY_NOOP
	case LogEntryConfig:
		return pb.LogEntryType_LOG_ENTRY_CONFIG
	default:
		return pb.LogEntryType_LOG_ENTRY_COMMAND
	}
}

// protoTypeToLogEntryType converts a protobuf LogEntryType to raft.LogEntryType.
func protoTypeToLogEntryType(t pb.LogEntryType) LogEntryType {
	switch t {
	case pb.LogEntryType_LOG_ENTRY_NOOP:
		return LogEntryNoop
	case pb.LogEntryType_LOG_ENTRY_CONFIG:
		return LogEntryConfig
	default:
		return LogEntryCommand
	}
}

func stringifyResult(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// GRPCPeer implements the Peer interface over a live gRPC connection.
// Used by cmd/node to wire each node to its peers after dialling them.
type GRPCPeer struct {
	id     string
	client pb.RaftServiceClient
}

// NewGRPCPeer creates a Peer backed by a gRPC client connection.
func NewGRPCPeer(id string, client pb.RaftServiceClient) *GRPCPeer {
	return &GRPCPeer{id: id, client: client}
}

func (p *GRPCPeer) RequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	resp, err := p.client.RequestVote(ctx, &pb.RequestVoteArgs{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
			return RequestVoteReply{}, err
		}
		return RequestVoteReply{}, err
	}
	return RequestVoteReply{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (p *GRPCPeer) AppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	entries := make([]*pb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &pb.LogEntry{
			Term:        e.Term,
			Type:        logEntryTypeToProto(e.Type),
			Command:     e.Command,
			ClientId:    e.ClientID,
			SequenceNum: e.SequenceNum,
			ConfigData:  e.ConfigData,
		}
	}
	resp, err := p.client.AppendEntries(ctx, &pb.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// InstallSnapshot handles a chunked InstallSnapshot RPC from the current leader (§7).
func (s *Server) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	args := InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIndex,
		LastIncludedTerm:  req.LastTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
		Configuration:     protoToClusterConfig(req.Configuration),
	}
	reply := s.node.InstallSnapshot(args)
	return &pb.InstallSnapshotReply{Term: reply.Term}, nil
}

// ── ClusterConfig ↔ proto helpers ────────────────────────────────────────────

func clusterConfigToProto(cfg *ClusterConfig) *pb.ClusterConfigProto {
	if cfg == nil {
		return nil
	}
	members := make([]*pb.ClusterMemberProto, len(cfg.Members))
	for i, m := range cfg.Members {
		members[i] = &pb.ClusterMemberProto{Id: m.ID, Role: int32(m.Role)}
	}
	return &pb.ClusterConfigProto{Members: members}
}

func protoToClusterConfig(p *pb.ClusterConfigProto) *ClusterConfig {
	if p == nil {
		return nil
	}
	members := make([]ClusterMember, len(p.Members))
	for i, m := range p.Members {
		members[i] = ClusterMember{ID: m.Id, Role: MemberRole(m.Role)}
	}
	return &ClusterConfig{Members: members}
}

func (p *GRPCPeer) PreVote(ctx context.Context, args PreVoteArgs) (PreVoteReply, error) {
	resp, err := p.client.PreVote(ctx, &pb.PreVoteArgs{
		NextTerm:     args.NextTerm,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return PreVoteReply{}, err
	}
	return PreVoteReply{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// InstallSnapshot sends a single InstallSnapshot chunk RPC to the peer (§7).
func (p *GRPCPeer) InstallSnapshot(ctx context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	resp, err := p.client.InstallSnapshot(ctx, &pb.InstallSnapshotArgs{
		Term:          args.Term,
		LeaderId:      args.LeaderID,
		LastIndex:     args.LastIncludedIndex,
		LastTerm:      args.LastIncludedTerm,
		Offset:        args.Offset,
		Data:          args.Data,
		Done:          args.Done,
		Configuration: clusterConfigToProto(args.Configuration),
	})
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	return InstallSnapshotReply{Term: resp.Term}, nil
}
