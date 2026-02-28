package raft

import (
	"sync"
	"testing"
)

func TestMetricsSnapshot_ReflectsCounterUpdates(t *testing.T) {
	metrics := newMetrics()

	metrics.incElectionsStarted()
	metrics.incElectionsWon()
	metrics.incCommandsSubmitted()
	metrics.incCommandsApplied()
	metrics.addLogEntriesReplicated(3)
	metrics.incReadIndexRequests()

	snap := metrics.snapshot()
	if snap.ElectionsStarted != 1 {
		t.Fatalf("ElectionsStarted = %d, want 1", snap.ElectionsStarted)
	}
	if snap.ElectionsWon != 1 {
		t.Fatalf("ElectionsWon = %d, want 1", snap.ElectionsWon)
	}
	if snap.CommandsSubmitted != 1 {
		t.Fatalf("CommandsSubmitted = %d, want 1", snap.CommandsSubmitted)
	}
	if snap.CommandsApplied != 1 {
		t.Fatalf("CommandsApplied = %d, want 1", snap.CommandsApplied)
	}
	if snap.LogEntriesReplicated != 3 {
		t.Fatalf("LogEntriesReplicated = %d, want 3", snap.LogEntriesReplicated)
	}
	if snap.ReadIndexRequests != 1 {
		t.Fatalf("ReadIndexRequests = %d, want 1", snap.ReadIndexRequests)
	}
}

func TestMetricsCounters_AreSafeUnderConcurrentUpdates(t *testing.T) {
	metrics := newMetrics()

	const goroutines = 16
	const incrementsPerGoroutine = 250

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				metrics.incCommandsSubmitted()
				metrics.incCommandsApplied()
				metrics.addLogEntriesReplicated(1)
				metrics.incReadIndexRequests()
			}
		}()
	}

	wg.Wait()

	want := int64(goroutines * incrementsPerGoroutine)
	snap := metrics.snapshot()
	if snap.CommandsSubmitted != want {
		t.Fatalf("CommandsSubmitted = %d, want %d", snap.CommandsSubmitted, want)
	}
	if snap.CommandsApplied != want {
		t.Fatalf("CommandsApplied = %d, want %d", snap.CommandsApplied, want)
	}
	if snap.LogEntriesReplicated != want {
		t.Fatalf("LogEntriesReplicated = %d, want %d", snap.LogEntriesReplicated, want)
	}
	if snap.ReadIndexRequests != want {
		t.Fatalf("ReadIndexRequests = %d, want %d", snap.ReadIndexRequests, want)
	}
}

func TestSnapshot_ExportsMetrics(t *testing.T) {
	metrics := newMetrics()
	metrics.incElectionsStarted()
	metrics.incCommandsSubmitted()

	node := &Node{
		id:      "A",
		state:   Follower,
		metrics: metrics,
	}

	snap := node.snapshot()
	if snap.Metrics.ElectionsStarted != 1 {
		t.Fatalf("snapshot metrics ElectionsStarted = %d, want 1",
			snap.Metrics.ElectionsStarted)
	}
	if snap.Metrics.CommandsSubmitted != 1 {
		t.Fatalf("snapshot metrics CommandsSubmitted = %d, want 1",
			snap.Metrics.CommandsSubmitted)
	}
}
