package raft

import "time"

// RpcEvent represents a single RPC message for visualization.
type RpcEvent struct {
	FromNode  string    // sender node ID
	ToNode    string    // recipient node ID
	RpcType   string    // "APPEND_ENTRIES", "REQUEST_VOTE", etc.
	EventTime time.Time // when the RPC was sent/received
}

// RpcObserver receives RPC event notifications from a Raft node.
//
// This interface allows external systems (like visualization gateways) to
// observe RPC traffic without coupling the core Raft protocol to any
// particular transport or observer implementation.
//
// Implementations must be non-blocking as they are called during RPC processing.
type RpcObserver interface {
	// OnRpcSend is called when the node sends an RPC to a peer.
	OnRpcSend(event RpcEvent)

	// OnRpcReceive is called when the node receives an RPC from a peer.
	OnRpcReceive(event RpcEvent)
}

// rpcObserverList is a helper for managing multiple RPC observers.
type rpcObserverList []RpcObserver

// notifySend sends the event to all observers for outgoing RPCs.
func (list rpcObserverList) notifySend(event RpcEvent) {
	for _, obs := range list {
		obs.OnRpcSend(event)
	}
}

// notifyReceive sends the event to all observers for incoming RPCs.
func (list rpcObserverList) notifyReceive(event RpcEvent) {
	for _, obs := range list {
		obs.OnRpcReceive(event)
	}
}
