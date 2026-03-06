package gateway

import (
	"context"
	"testing"
	"time"

	pb "github.com/mehdiakiki/raft-core/gen/raft"
	"github.com/mehdiakiki/raft-core/internal/raft"
	"google.golang.org/grpc"
)

type mockGatewayClient struct {
	pushRpcCh chan *pb.RaftRpcEvent
}

func (m *mockGatewayClient) PushState(context.Context, *pb.RaftStateEvent, ...grpc.CallOption) (*pb.PushStateAck, error) {
	return &pb.PushStateAck{Received: true}, nil
}

func (m *mockGatewayClient) PushRpc(_ context.Context, in *pb.RaftRpcEvent, _ ...grpc.CallOption) (*pb.PushRpcAck, error) {
	select {
	case m.pushRpcCh <- in:
	default:
	}
	return &pb.PushRpcAck{Received: true}, nil
}

func TestPusher_OnRpcSend_MapsAllRpcMetadataFields(t *testing.T) {
	client := &mockGatewayClient{pushRpcCh: make(chan *pb.RaftRpcEvent, 1)}
	pusher := &Pusher{nodeID: "A", client: client}

	voteGranted := true
	term := int64(9)
	ts := time.UnixMilli(123456)
	pusher.OnRpcSend(raft.RpcEvent{
		FromNode:    "B",
		ToNode:      "A",
		RpcType:     "VOTE_REPLY",
		RpcID:       "rv:reply:9:B:A",
		EventTime:   ts,
		Term:        term,
		HasTerm:     true,
		CandidateID: "A",
		VoteGranted: &voteGranted,
		Direction:   raft.RpcDirectionSend,
	})

	select {
	case event := <-client.pushRpcCh:
		if event.FromNode != "B" || event.ToNode != "A" {
			t.Fatalf("unexpected endpoints: from=%s to=%s", event.FromNode, event.ToNode)
		}
		if event.RpcType != "VOTE_REPLY" {
			t.Fatalf("unexpected rpc type: %s", event.RpcType)
		}
		if event.EventTimeMs != ts.UnixMilli() {
			t.Fatalf("unexpected event time: %d", event.EventTimeMs)
		}
		if event.RpcId != "rv:reply:9:B:A" {
			t.Fatalf("unexpected rpc_id: %s", event.RpcId)
		}
		if event.Term == nil || *event.Term != 9 {
			t.Fatalf("unexpected term pointer: %+v", event.Term)
		}
		if event.CandidateId == nil || *event.CandidateId != "A" {
			t.Fatalf("unexpected candidate_id pointer: %+v", event.CandidateId)
		}
		if event.VoteGranted == nil || !*event.VoteGranted {
			t.Fatalf("unexpected vote_granted pointer: %+v", event.VoteGranted)
		}
		if event.Direction == nil || *event.Direction != raft.RpcDirectionSend {
			t.Fatalf("unexpected direction pointer: %+v", event.Direction)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for pushed RPC event")
	}
}

func TestPusher_OnRpcReceive_DoesNotPushToGateway(t *testing.T) {
	client := &mockGatewayClient{pushRpcCh: make(chan *pb.RaftRpcEvent, 1)}
	pusher := &Pusher{nodeID: "A", client: client}

	pusher.OnRpcReceive(raft.RpcEvent{
		FromNode:  "A",
		ToNode:    "B",
		RpcType:   "REQUEST_VOTE",
		RpcID:     "rv:req:2:A:B",
		EventTime: time.UnixMilli(42),
	})

	select {
	case event := <-client.pushRpcCh:
		t.Fatalf("expected no pushed event for receive-side observation, got %+v", event)
	case <-time.After(120 * time.Millisecond):
	}
}
