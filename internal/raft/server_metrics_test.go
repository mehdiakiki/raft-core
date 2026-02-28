package raft

import "testing"

func TestSnapshotToProto_MapsMetrics(t *testing.T) {
	snap := StateSnapshot{
		NodeID: "A",
		State:  Leader,
		Metrics: MetricsSnapshot{
			ElectionsStarted:     3,
			ElectionsWon:         2,
			CommandsSubmitted:    9,
			CommandsApplied:      8,
			LogEntriesReplicated: 21,
			ReadIndexRequests:    4,
		},
	}

	reply := snapshotToProto(snap)
	if reply.GetMetrics() == nil {
		t.Fatal("expected metrics in proto reply")
	}
	if reply.GetMetrics().GetElectionsStarted() != 3 {
		t.Fatalf("ElectionsStarted = %d, want 3", reply.GetMetrics().GetElectionsStarted())
	}
	if reply.GetMetrics().GetCommandsSubmitted() != 9 {
		t.Fatalf("CommandsSubmitted = %d, want 9", reply.GetMetrics().GetCommandsSubmitted())
	}
	if reply.GetMetrics().GetLogEntriesReplicated() != 21 {
		t.Fatalf("LogEntriesReplicated = %d, want 21", reply.GetMetrics().GetLogEntriesReplicated())
	}
}
