// Package storage provides persistent storage implementations for the Raft consensus algorithm.
package storage

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/medvih/raft-core/internal/raft"
	"go.etcd.io/bbolt"
)

const (
	bucketStable = "stable"
	bucketLog    = "log"
)

var (
	keyTerm        = []byte("term")
	keyVotedFor    = []byte("votedFor")
	keyCommitIndex = []byte("commitIndex")
)

type BoltStore struct {
	db   *bbolt.DB
	path string
}

func NewBoltStore(path string) (*BoltStore, error) {
	if path == "" {
		return nil, fmt.Errorf("bolt store: path is required")
	}

	// Ensure directory exists.
	dir := fmt.Sprintf("%s/", path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("bolt store: create directory: %w", err)
	}

	dbPath := dir + "raft.db"
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("bolt store: open database: %w", err)
	}

	// Create buckets if they don't exist.
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketStable)); err != nil {
			return fmt.Errorf("create stable bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketLog)); err != nil {
			return fmt.Errorf("create log bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("bolt store: initialize buckets: %w", err)
	}

	return &BoltStore{
		db:   db,
		path: path,
	}, nil
}

func (b *BoltStore) Close() error {
	return b.db.Close()
}

func (b *BoltStore) GetTerm() (int64, error) {
	var term int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		val := bucket.Get(keyTerm)
		if val == nil {
			term = 0
			return nil
		}
		term = int64(binary.BigEndian.Uint64(val))
		return nil
	})
	return term, err
}

func (b *BoltStore) SetTerm(term int64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(term))
		return bucket.Put(keyTerm, buf[:])
	})
}

func (b *BoltStore) GetVotedFor() (string, error) {
	var votedFor string
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		val := bucket.Get(keyVotedFor)
		if val == nil {
			votedFor = ""
			return nil
		}
		votedFor = string(val)
		return nil
	})
	return votedFor, err
}

func (b *BoltStore) SetVotedFor(id string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		if id == "" {
			return bucket.Delete(keyVotedFor)
		}
		return bucket.Put(keyVotedFor, []byte(id))
	})
}

func (b *BoltStore) GetCommitIndex() (int64, error) {
	var commitIndex int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		val := bucket.Get(keyCommitIndex)
		if val == nil {
			commitIndex = 0
			return nil
		}
		commitIndex = int64(binary.BigEndian.Uint64(val))
		return nil
	})
	return commitIndex, err
}

func (b *BoltStore) SetCommitIndex(index int64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketStable))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(index))
		return bucket.Put(keyCommitIndex, buf[:])
	})
}

// AppendLog appends entries to the end of the log.
// Not part of the raft.Storage interface; provided for tests and
// incremental construction of log state.
func (b *BoltStore) AppendLog(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLog))
		cursor := bucket.Cursor()
		lastKey, _ := cursor.Last()
		var nextIndex uint64 = 1
		if lastKey != nil {
			nextIndex = binary.BigEndian.Uint64(lastKey) + 1
		}
		for i, entry := range entries {
			val, err := encodeLogEntry(entry)
			if err != nil {
				return fmt.Errorf("encode log entry: %w", err)
			}
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], nextIndex+uint64(i))
			if err := bucket.Put(key[:], val); err != nil {
				return fmt.Errorf("put log entry: %w", err)
			}
		}
		return nil
	})
}

// StoreLog implements raft.Storage.
// It atomically replaces the entire log: deletes all existing entries then
// writes the new ones, all within a single BoltDB transaction.
func (b *BoltStore) StoreLog(entries []raft.LogEntry) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLog))

		// Delete all existing entries.
		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("clear log: %w", err)
			}
		}

		// Write new entries with 1-based index keys.
		for i, entry := range entries {
			val, err := encodeLogEntry(entry)
			if err != nil {
				return fmt.Errorf("encode log entry at index %d: %w", i+1, err)
			}
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(i+1))
			if err := bucket.Put(key[:], val); err != nil {
				return fmt.Errorf("put log entry at index %d: %w", i+1, err)
			}
		}
		return nil
	})
}

func (b *BoltStore) GetLog(start, end int64) ([]raft.LogEntry, error) {
	if start >= end {
		return nil, nil
	}

	var entries []raft.LogEntry
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLog))
		cursor := bucket.Cursor()

		// Seek to start.
		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(start))
		for k, v := cursor.Seek(startKey); k != nil; k, v = cursor.Next() {
			index := int64(binary.BigEndian.Uint64(k))
			if index >= end {
				break
			}

			entry, err := decodeLogEntry(v)
			if err != nil {
				return fmt.Errorf("decode log entry at index %d: %w", index, err)
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

func (b *BoltStore) TruncateLog(index int64) error {
	if index <= 0 {
		return nil
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLog))
		cursor := bucket.Cursor()

		// Delete all entries from index onwards.
		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(index))
		for k, _ := cursor.Seek(startKey); k != nil; k, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("delete log entry: %w", err)
			}
		}
		return nil
	})
}

func (b *BoltStore) LastLogIndexAndTerm() (int64, int64) {
	var index int64
	var term int64

	_ = b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLog))
		cursor := bucket.Cursor()
		k, v := cursor.Last()
		if k == nil {
			return nil
		}

		index = int64(binary.BigEndian.Uint64(k))

		entry, err := decodeLogEntry(v)
		if err != nil {
			return err
		}
		term = entry.Term
		return nil
	})

	return index, term
}

func encodeLogEntry(entry raft.LogEntry) ([]byte, error) {
	// Binary layout (v3):
	//   term           (8 bytes, big-endian int64)
	//   type           (1 byte)
	//   seqNum         (8 bytes, big-endian int64)
	//   clientIDLen    (4 bytes, big-endian uint32)
	//   clientID       (clientIDLen bytes)
	//   cmdLen         (4 bytes, big-endian uint32)
	//   command        (cmdLen bytes)
	//   configDataLen  (4 bytes, big-endian uint32)  ← new in v3
	//   configData     (configDataLen bytes)          ← new in v3
	clientID := []byte(entry.ClientID)
	cmd := []byte(entry.Command)
	cfgData := entry.ConfigData
	size := 8 + 1 + 8 + 4 + len(clientID) + 4 + len(cmd) + 4 + len(cfgData)
	buf := make([]byte, size)

	off := 0
	binary.BigEndian.PutUint64(buf[off:], uint64(entry.Term))
	off += 8
	buf[off] = byte(entry.Type)
	off++
	binary.BigEndian.PutUint64(buf[off:], uint64(entry.SequenceNum))
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(len(clientID)))
	off += 4
	copy(buf[off:], clientID)
	off += len(clientID)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(cmd)))
	off += 4
	copy(buf[off:], cmd)
	off += len(cmd)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(cfgData)))
	off += 4
	copy(buf[off:], cfgData)

	return buf, nil
}

func decodeLogEntry(data []byte) (raft.LogEntry, error) {
	// Detect format by header size:
	//   v0 (legacy): 12 bytes  (term 8 + cmdLen 4)
	//   v1:          13 bytes  (term 8 + type 1 + cmdLen 4)
	//   v2:          25+ bytes (term 8 + type 1 + seqNum 8 + clientIDLen 4 + cmdLen 4)
	//   v3:          29+ bytes (v2 header + configDataLen 4)

	if len(data) < 13 {
		// v0 legacy: no type byte.
		if len(data) < 12 {
			return raft.LogEntry{}, fmt.Errorf("log entry too short: %d bytes", len(data))
		}
		term := int64(binary.BigEndian.Uint64(data[0:8]))
		cmdLen := int(binary.BigEndian.Uint32(data[8:12]))
		if len(data) < 12+cmdLen {
			return raft.LogEntry{}, fmt.Errorf("log entry truncated (v0)")
		}
		return raft.LogEntry{Term: term, Type: raft.LogEntryCommand, Command: string(data[12 : 12+cmdLen])}, nil
	}

	// v1: type byte, but no seqNum/clientID fields.
	const v2MinHeader = 8 + 1 + 8 + 4 + 4 // term + type + seqNum + clientIDLen + cmdLen
	if len(data) < v2MinHeader {
		term := int64(binary.BigEndian.Uint64(data[0:8]))
		entryType := raft.LogEntryType(data[8])
		cmdLen := int(binary.BigEndian.Uint32(data[9:13]))
		if len(data) < 13+cmdLen {
			return raft.LogEntry{}, fmt.Errorf("log entry truncated (v1)")
		}
		return raft.LogEntry{Term: term, Type: entryType, Command: string(data[13 : 13+cmdLen])}, nil
	}

	// v2 / v3: full header with seqNum, clientID, command.
	off := 0
	term := int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	entryType := raft.LogEntryType(data[off])
	off++
	seqNum := int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	clientIDLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	if len(data) < off+clientIDLen+4 {
		return raft.LogEntry{}, fmt.Errorf("log entry truncated (v2 clientID)")
	}
	clientID := string(data[off : off+clientIDLen])
	off += clientIDLen
	cmdLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	if len(data) < off+cmdLen {
		return raft.LogEntry{}, fmt.Errorf("log entry truncated (v2 command)")
	}
	cmd := string(data[off : off+cmdLen])
	off += cmdLen

	// v3: configDataLen + configData (absent in v2 entries on disk).
	var cfgData []byte
	if len(data) >= off+4 {
		cfgLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if cfgLen > 0 {
			if len(data) < off+cfgLen {
				return raft.LogEntry{}, fmt.Errorf("log entry truncated (v3 configData)")
			}
			cfgData = make([]byte, cfgLen)
			copy(cfgData, data[off:off+cfgLen])
		}
	}

	return raft.LogEntry{
		Term:        term,
		Type:        entryType,
		SequenceNum: seqNum,
		ClientID:    clientID,
		Command:     cmd,
		ConfigData:  cfgData,
	}, nil
}

var _ raft.Storage = (*BoltStore)(nil)
