package gateway

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/mehdiakiki/raft-core/gen/raft"
	"github.com/mehdiakiki/raft-core/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Pusher struct {
	nodeID string
	client pb.RaftGatewayClient
	conn   *grpc.ClientConn
	mu     sync.Mutex
}

func NewPusher(nodeID, gatewayAddr string) (*Pusher, error) {
	conn, err := grpc.NewClient(gatewayAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Pusher{
		nodeID: nodeID,
		client: pb.NewRaftGatewayClient(conn),
		conn:   conn,
	}, nil
}

func (p *Pusher) OnStateChange(snapshot raft.StateSnapshot) {
	event := p.snapshotToEvent(snapshot.ToLite())

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := p.client.PushState(ctx, event)
		if err != nil {
			slog.Debug("gateway push failed", "node", p.nodeID, "err", err)
		}
	}()
}

func (p *Pusher) snapshotToEvent(lite raft.StateSnapshotLite) *pb.RaftStateEvent {
	state := lite.State.String()

	return &pb.RaftStateEvent{
		NodeId:              lite.NodeID,
		State:               &state,
		CurrentTerm:         &lite.CurrentTerm,
		VotedFor:            ptrString(lite.VotedFor),
		EventTimeMs:         lite.EventTimeMs,
		CommitIndex:         &lite.CommitIndex,
		LastApplied:         &lite.LastApplied,
		LeaderId:            ptrString(lite.LeaderID),
		HeartbeatIntervalMs: ptrInt64(lite.HeartbeatIntervalMs),
		ElectionTimeoutMs:   ptrInt64(lite.ElectionTimeoutMs),
	}
}

func (p *Pusher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func ptrString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func ptrInt64(v int64) *int64 {
	return &v
}
