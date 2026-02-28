package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

// KVCommand is a JSON-encoded key-value store operation.
//
// Recognised operations are "set" and "delete".
type KVCommand struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

var (
	// ErrInvalidCommand is returned by KVStore.Apply for malformed JSON commands.
	ErrInvalidCommand = errors.New("kvstore: invalid command JSON")
	// ErrNilReader is returned by KVStore.Restore when a nil reader is given.
	ErrNilReader = errors.New("kvstore: nil reader provided to Restore")
)

// KVStore is a thread-safe in-memory key-value store that implements StateMachine.
//
// Commands are JSON-encoded KVCommand objects.
// Snapshot produces a JSON map; Restore accepts the same format.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore returns an empty KVStore.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

// Apply executes a JSON-encoded KVCommand against the store.
//
// Recognised operations: "set", "delete".
// Unknown operations are silently ignored (idempotent forward compatibility).
// Returns ErrInvalidCommand for malformed JSON.
func (kv *KVStore) Apply(command string) interface{} {
	var cmd KVCommand
	if err := json.Unmarshal([]byte(command), &cmd); err != nil {
		return ErrInvalidCommand
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "set":
		kv.data[cmd.Key] = cmd.Value
	case "delete":
		delete(kv.data, cmd.Key)
	}
	return nil
}

// Get retrieves a value from the store.
// Returns the value and true if the key exists.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Len returns the number of keys currently in the store.
func (kv *KVStore) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// Snapshot serialises the current state as a JSON map.
//
// The returned reader contains the full map; the caller must close it.
// The snapshot is taken under a read lock, then encoded without the lock
// to minimise contention.
func (kv *KVStore) Snapshot() (io.ReadCloser, error) {
	kv.mu.RLock()
	clone := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		clone[k] = v
	}
	kv.mu.RUnlock()

	data, err := json.Marshal(clone)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Restore atomically replaces the store's state from a JSON snapshot.
//
// On error the previous state is preserved.
// The reader is closed on success.
func (kv *KVStore) Restore(rc io.ReadCloser) error {
	if rc == nil {
		return ErrNilReader
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var newData map[string]string
	if err := json.Unmarshal(data, &newData); err != nil {
		return ErrInvalidCommand
	}
	kv.mu.Lock()
	kv.data = newData
	kv.mu.Unlock()
	return rc.Close()
}

// Compile-time check: KVStore implements StateMachine.
var _ StateMachine = (*KVStore)(nil)
