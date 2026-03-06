package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

type captureRpcObserver struct {
	mu     sync.Mutex
	events []RpcEvent
}

func (c *captureRpcObserver) OnRpcSend(event RpcEvent) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
}

func (c *captureRpcObserver) OnRpcReceive(event RpcEvent) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
}

func (c *captureRpcObserver) snapshot() []RpcEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RpcEvent, len(c.events))
	copy(out, c.events)
	return out
}

type voteMockPeer struct {
	reply RequestVoteReply
	err   error
}

func (m *voteMockPeer) RequestVote(_ context.Context, _ RequestVoteArgs) (RequestVoteReply, error) {
	return m.reply, m.err
}

func (m *voteMockPeer) AppendEntries(_ context.Context, _ AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{Success: true}, nil
}

func (m *voteMockPeer) PreVote(_ context.Context, args PreVoteArgs) (PreVoteReply, error) {
	return PreVoteReply{Term: args.NextTerm, VoteGranted: true}, nil
}

func findEvent(t *testing.T, events []RpcEvent, rpcType, direction string) RpcEvent {
	t.Helper()
	for _, event := range events {
		if event.RpcType == rpcType && event.Direction == direction {
			return event
		}
	}
	t.Fatalf("event not found: type=%s direction=%s events=%v", rpcType, direction, events)
	return RpcEvent{}
}

func TestRequestVote_EmitsMetadataRichRequestAndReplyEvents(t *testing.T) {
	observer := &captureRpcObserver{}
	node := New(Config{
		ID:           "B",
		RpcObservers: []RpcObserver{observer},
	})

	reply := node.RequestVote(RequestVoteArgs{
		Term:         4,
		CandidateID:  "A",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !reply.VoteGranted {
		t.Fatal("expected granted vote in setup")
	}

	events := observer.snapshot()
	requestEvent := findEvent(t, events, "REQUEST_VOTE", RpcDirectionReceive)
	if requestEvent.RpcID != requestVoteRPCID(4, "A", "B") {
		t.Fatalf("unexpected request rpc_id: %s", requestEvent.RpcID)
	}
	if !requestEvent.HasTerm || requestEvent.Term != 4 {
		t.Fatalf("expected request term=4 with HasTerm=true, got term=%d has=%t", requestEvent.Term, requestEvent.HasTerm)
	}
	if requestEvent.CandidateID != "A" {
		t.Fatalf("unexpected request candidate_id: %q", requestEvent.CandidateID)
	}

	replyEvent := findEvent(t, events, "VOTE_REPLY", RpcDirectionSend)
	if replyEvent.RpcID != voteReplyRPCID(4, "B", "A") {
		t.Fatalf("unexpected reply rpc_id: %s", replyEvent.RpcID)
	}
	if !replyEvent.HasTerm || replyEvent.Term != 4 {
		t.Fatalf("expected reply term=4 with HasTerm=true, got term=%d has=%t", replyEvent.Term, replyEvent.HasTerm)
	}
	if replyEvent.CandidateID != "A" {
		t.Fatalf("unexpected reply candidate_id: %q", replyEvent.CandidateID)
	}
	if replyEvent.VoteGranted == nil || !*replyEvent.VoteGranted {
		t.Fatalf("expected vote_granted=true, got %+v", replyEvent.VoteGranted)
	}
}

func TestRequestVote_DeniedVoteReplyCarriesVoteGrantedFalse(t *testing.T) {
	observer := &captureRpcObserver{}
	node := New(Config{
		ID:           "B",
		RpcObservers: []RpcObserver{observer},
	})

	_ = node.RequestVote(RequestVoteArgs{Term: 5, CandidateID: "A"})
	reply := node.RequestVote(RequestVoteArgs{Term: 5, CandidateID: "C"})
	if reply.VoteGranted {
		t.Fatal("expected second vote in same term to be denied")
	}

	events := observer.snapshot()
	var denied *RpcEvent
	for i := range events {
		event := events[i]
		if event.RpcType == "VOTE_REPLY" &&
			event.Direction == RpcDirectionSend &&
			event.CandidateID == "C" {
			denied = &event
		}
	}
	if denied == nil {
		t.Fatalf("expected denied vote reply event, got events=%v", events)
	}
	if denied.RpcID != voteReplyRPCID(5, "B", "C") {
		t.Fatalf("unexpected denied rpc_id: %s", denied.RpcID)
	}
	if denied.VoteGranted == nil || *denied.VoteGranted {
		t.Fatalf("expected vote_granted=false, got %+v", denied.VoteGranted)
	}
}

func TestStartElection_EmitsRequestVoteSendMetadata(t *testing.T) {
	observer := &captureRpcObserver{}
	node := New(Config{
		ID: "A",
		Peers: map[string]Peer{
			"B": &voteMockPeer{reply: RequestVoteReply{Term: 1, VoteGranted: false}},
		},
		RpcObservers: []RpcObserver{observer},
	})

	node.startElection(context.Background())

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		events := observer.snapshot()
		for _, event := range events {
			if event.RpcType == "REQUEST_VOTE" && event.Direction == RpcDirectionSend {
				if event.FromNode != "A" || event.ToNode != "B" {
					t.Fatalf("unexpected request event endpoints: %+v", event)
				}
				if event.RpcID != requestVoteRPCID(1, "A", "B") {
					t.Fatalf("unexpected request rpc_id: %s", event.RpcID)
				}
				if !event.HasTerm || event.Term != 1 {
					t.Fatalf("expected term=1 in send event, got term=%d has=%t", event.Term, event.HasTerm)
				}
				if event.CandidateID != "A" {
					t.Fatalf("unexpected candidate_id in send event: %q", event.CandidateID)
				}
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for REQUEST_VOTE send event; events=%v", events)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
