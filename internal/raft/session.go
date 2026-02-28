package raft

// session.go — client session tracking for exactly-once semantics (§8).
//
// The Raft paper §8 ("Client Interaction") requires that each client request
// carry a unique serial number so the leader can detect retries and return
// the cached result instead of applying the command a second time.
//
// This file is intentionally small: it owns only the read and write paths for
// the clientSessions map.  The map itself lives on Node so that it is
// accessible from the apply loop and from SubmitCommand.

// isDuplicate reports whether the given (clientID, seqNum) pair has already
// been applied, and if so returns the cached result.
//
// A request is a duplicate when seqNum <= the last applied sequence number for
// that client.  We also return (true, result) when seqNum equals the last
// applied number so the caller gets the original response.
//
// Returns (false, nil) when clientID is empty or seqNum is 0 — both signal
// that the caller opted out of deduplication.
//
// Caller must hold n.mu.
func (n *Node) isDuplicate(clientID string, seqNum int64) (bool, interface{}) {
	if clientID == "" || seqNum == 0 {
		return false, nil
	}
	session, exists := n.clientSessions[clientID]
	if !exists {
		return false, nil
	}
	if seqNum <= session.LastSeqNum {
		return true, session.LastResult
	}
	return false, nil
}

// recordSession updates the client session after successfully applying a command.
//
// Only updates when the incoming seqNum is strictly greater than what we have
// stored, so out-of-order apply callbacks from tests cannot regress the counter.
//
// Caller must hold n.mu.
func (n *Node) recordSession(clientID string, seqNum int64, result interface{}) {
	if clientID == "" || seqNum == 0 {
		return
	}
	session := n.clientSessions[clientID]
	if seqNum > session.LastSeqNum {
		n.clientSessions[clientID] = ClientSession{
			LastSeqNum: seqNum,
			LastResult: result,
		}
	}
}

// sessionResultForExactSeq returns the cached result only when the stored
// sequence exactly matches seqNum.
//
// This is used by synchronous submit responses so we don't conflate a stale
// request (older seq) with a retry of the latest request.
func (n *Node) sessionResultForExactSeq(clientID string, seqNum int64) (interface{}, bool) {
	if clientID == "" || seqNum == 0 {
		return nil, false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	session, exists := n.clientSessions[clientID]
	if !exists || session.LastSeqNum != seqNum {
		return nil, false
	}
	return session.LastResult, true
}
