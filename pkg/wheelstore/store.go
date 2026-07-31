// Package wheelstore persists scheduler timer-wheel slots to bbolt so a pod
// restart can rebuild the in-memory wheel without re-polling Postgres.
//
// Keys are "MM:SS" strings (e.g. "32:47"). Values are packed fixed-width
// entries with no delimiters — N entries fit in exactly entryLen*N bytes. Each
// entry is a 16-byte UUIDv7 id followed by its deliver_at as an 8-byte
// big-endian Unix timestamp.
//
// The timestamp is what makes recovery honest. The key alone carries no hour or
// date, so a pod that was down across an hour boundary cannot tell a slot that
// fired 50 minutes ago from one due in 10 — it would restore the stale slot as
// "future" and fire it late. With deliver_at in the value, recovery can simply
// drop everything already past.
package wheelstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	// idLen is the byte width of a UUID in a packed entry.
	idLen = 16
	// tsLen is the byte width of the deliver_at Unix seconds that follow it.
	tsLen = 8
	// entryLen is one complete packed entry.
	entryLen = idLen + tsLen
)

// bucketName is the single bbolt bucket all slots live in.
var bucketName = []byte("wheel")

// Entry is one persisted schedule: its id and when it is due.
type Entry struct {
	ID        [idLen]byte
	DeliverAt time.Time
}

// Store wraps a bbolt DB scoped to the scheduler's wheel state.
type Store struct {
	db *bolt.DB
}

// Open opens (or creates) the bbolt file at path and ensures the wheel bucket exists.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("bbolt open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bbolt bucket: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying bbolt file handle.
func (s *Store) Close() error { return s.db.Close() }

// Append appends an entry to the slot's packed value in a single write
// transaction. Concurrent appends to the same slot are serialised by bbolt's
// writer lock.
func (s *Store) Append(slot string, id [idLen]byte, deliverAt time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		key := []byte(slot)
		// bbolt's Get returns a slice owned by the mmap — copy before mutation.
		existing := b.Get(key)
		next := make([]byte, 0, len(existing)+entryLen)
		next = append(next, existing...)
		next = append(next, encodeEntry(Entry{ID: id, DeliverAt: deliverAt})...)
		return b.Put(key, next)
	})
}

// Delete removes a slot key entirely. No-op if the key is absent.
func (s *Store) Delete(slot string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(slot))
	})
}

// Range invokes fn for every slot present in the store. Decoding errors abort
// the iteration. Used by recovery on pod startup.
func (s *Store) Range(fn func(slot string, entries []Entry) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			entries, err := decode(v)
			if err != nil {
				return fmt.Errorf("slot %s: %w", string(k), err)
			}
			return fn(string(k), entries)
		})
	})
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, entryLen)
	copy(buf[:idLen], e.ID[:])
	binary.BigEndian.PutUint64(buf[idLen:], uint64(e.DeliverAt.Unix()))
	return buf
}

// decode unpacks a slot value into its entries.
func decode(v []byte) ([]Entry, error) {
	if len(v)%entryLen != 0 {
		return nil, errors.New("slot value not aligned to entry width")
	}
	out := make([]Entry, 0, len(v)/entryLen)
	for chunk := range slices.Chunk(v, entryLen) {
		var e Entry
		copy(e.ID[:], chunk[:idLen])
		e.DeliverAt = time.Unix(int64(binary.BigEndian.Uint64(chunk[idLen:])), 0)
		out = append(out, e)
	}
	return out, nil
}
