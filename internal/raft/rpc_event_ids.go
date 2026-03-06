package raft

import "fmt"

func requestVoteRPCID(term int64, candidateID, voterID string) string {
	return fmt.Sprintf("rv:req:%d:%s:%s", term, candidateID, voterID)
}

func voteReplyRPCID(term int64, voterID, candidateID string) string {
	return fmt.Sprintf("rv:reply:%d:%s:%s", term, voterID, candidateID)
}

func preVoteRPCID(term int64, candidateID, voterID string) string {
	return fmt.Sprintf("pv:req:%d:%s:%s", term, candidateID, voterID)
}

func preVoteReplyRPCID(term int64, voterID, candidateID string) string {
	return fmt.Sprintf("pv:reply:%d:%s:%s", term, voterID, candidateID)
}

func appendEntriesRPCID(term int64, leaderID, followerID string, heartbeatRound uint64) string {
	return fmt.Sprintf("ae:req:%d:%s:%s:%d", term, leaderID, followerID, heartbeatRound)
}

func boolPtr(v bool) *bool {
	return &v
}
