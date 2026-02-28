package raft_test

// Tests for the KVStore state machine implementation.
//
// These tests verify that Apply, Get, Snapshot, and Restore behave correctly
// in isolation and that the KVStore correctly implements the StateMachine
// interface required by the Raft node.

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/medvih/raft-core/internal/raft"
)

// ── Apply ──────────────────────────────────────────────────────────────────────

// Apply "set" must store the key-value pair in the store.
func TestKVStore_Apply_Set_StoresKeyValue(t *testing.T) {
	kv := raft.NewKVStore()

	result := kv.Apply(`{"op":"set","key":"x","value":"42"}`)
	if result != nil {
		t.Errorf("Apply set: expected nil result, got %v", result)
	}

	val, ok := kv.Get("x")
	if !ok {
		t.Fatal("key x not found after set")
	}
	if val != "42" {
		t.Errorf("expected value 42, got %q", val)
	}
}

// Apply "delete" must remove an existing key.
func TestKVStore_Apply_Delete_RemovesKey(t *testing.T) {
	kv := raft.NewKVStore()
	kv.Apply(`{"op":"set","key":"y","value":"hello"}`)

	kv.Apply(`{"op":"delete","key":"y"}`)

	_, ok := kv.Get("y")
	if ok {
		t.Error("key y should not exist after delete")
	}
}

// Apply with an unknown operation must be silently ignored.
func TestKVStore_Apply_UnknownOp_IsIgnored(t *testing.T) {
	kv := raft.NewKVStore()
	kv.Apply(`{"op":"set","key":"a","value":"1"}`)

	result := kv.Apply(`{"op":"increment","key":"a"}`)
	if result != nil {
		t.Errorf("expected nil for unknown op, got %v", result)
	}

	// Original key should be untouched.
	val, _ := kv.Get("a")
	if val != "1" {
		t.Errorf("expected a=1 after unknown op, got %q", val)
	}
}

// Apply with malformed JSON must return ErrInvalidCommand.
func TestKVStore_Apply_MalformedJSON_ReturnsError(t *testing.T) {
	kv := raft.NewKVStore()

	result := kv.Apply("not-json{{{")
	if result == nil {
		t.Fatal("expected error result for malformed JSON, got nil")
	}
}

// Multiple sequential applies must be reflected in order.
func TestKVStore_Apply_MultipleOps_AppliedInOrder(t *testing.T) {
	kv := raft.NewKVStore()

	kv.Apply(`{"op":"set","key":"k","value":"1"}`)
	kv.Apply(`{"op":"set","key":"k","value":"2"}`)
	kv.Apply(`{"op":"set","key":"k","value":"3"}`)

	val, _ := kv.Get("k")
	if val != "3" {
		t.Errorf("expected k=3 after three sets, got %q", val)
	}
}

// ── Snapshot / Restore ─────────────────────────────────────────────────────────

// Snapshot must return a non-empty reader that deserialises to the current state.
func TestKVStore_Snapshot_ReturnsMarshalledState(t *testing.T) {
	kv := raft.NewKVStore()
	kv.Apply(`{"op":"set","key":"a","value":"1"}`)
	kv.Apply(`{"op":"set","key":"b","value":"2"}`)

	rc, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if m["a"] != "1" || m["b"] != "2" {
		t.Errorf("snapshot does not reflect current state: %v", m)
	}
}

// Restore must replace the current state with the snapshot data.
func TestKVStore_Restore_ReplacesCurrentState(t *testing.T) {
	kv := raft.NewKVStore()
	kv.Apply(`{"op":"set","key":"old","value":"data"}`)

	snapshotData := `{"new":"value"}`
	rc := io.NopCloser(strings.NewReader(snapshotData))

	if err := kv.Restore(rc); err != nil {
		t.Fatalf("Restore() error: %v", err)
	}

	// Old key must be gone.
	_, ok := kv.Get("old")
	if ok {
		t.Error("old key should not exist after Restore")
	}

	// New key must be present.
	val, ok := kv.Get("new")
	if !ok {
		t.Fatal("new key not found after Restore")
	}
	if val != "value" {
		t.Errorf("expected new=value, got %q", val)
	}
}

// Restore with a nil reader must return ErrNilReader.
func TestKVStore_Restore_NilReader_ReturnsError(t *testing.T) {
	kv := raft.NewKVStore()
	if err := kv.Restore(nil); err == nil {
		t.Error("expected error for nil reader, got nil")
	}
}

// Restore with invalid JSON must return an error and preserve original state.
func TestKVStore_Restore_InvalidJSON_PreservesState(t *testing.T) {
	kv := raft.NewKVStore()
	kv.Apply(`{"op":"set","key":"safe","value":"ok"}`)

	rc := io.NopCloser(bytes.NewReader([]byte("not-json")))
	if err := kv.Restore(rc); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}

	// Original state must be preserved.
	val, ok := kv.Get("safe")
	if !ok || val != "ok" {
		t.Errorf("expected safe=ok after failed restore, got %q %v", val, ok)
	}
}

// Snapshot round-trip: Snapshot → Restore → Get must produce the same state.
func TestKVStore_SnapshotRoundtrip_PreservesAllKeys(t *testing.T) {
	original := raft.NewKVStore()
	original.Apply(`{"op":"set","key":"alpha","value":"A"}`)
	original.Apply(`{"op":"set","key":"beta","value":"B"}`)
	original.Apply(`{"op":"set","key":"gamma","value":"C"}`)

	rc, err := original.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}

	restored := raft.NewKVStore()
	if err := restored.Restore(rc); err != nil {
		t.Fatalf("Restore(): %v", err)
	}

	for _, tc := range []struct{ key, want string }{
		{"alpha", "A"},
		{"beta", "B"},
		{"gamma", "C"},
	} {
		got, ok := restored.Get(tc.key)
		if !ok || got != tc.want {
			t.Errorf("key %q: expected %q, got %q (found=%v)", tc.key, tc.want, got, ok)
		}
	}
}
