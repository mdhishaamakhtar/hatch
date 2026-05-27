// Package wheelstore persists scheduler timer-wheel slots to bbolt so a pod
// restart can rebuild the in-memory wheel without re-polling Postgres.
//
// Keys are "MM:SS" strings (e.g. "32:47"). Values are packed [16]byte UUIDv7
// IDs concatenated with no delimiters — N IDs fit in exactly 16*N bytes. The
// LLD specifies this layout because it is ~55% smaller than UUID strings and
// the slot count (≤3600) is small enough that read-modify-write per insert is
// not a bottleneck.
package wheelstore

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// idLen is the byte width of a single packed UUID in a slot value.
const idLen = 16

// bucketName is the single bbolt bucket all slots live in.
var bucketName = []byte("wheel")

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
		_, e := tx.CreateBucketIfNotExists(bucketName)
		return e
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bbolt bucket: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying bbolt file handle.
func (s *Store) Close() error { return s.db.Close() }

// Append appends id to the slot's packed value in a single write transaction.
// Concurrent appends to the same slot are serialised by bbolt's writer lock.
func (s *Store) Append(slot string, id [idLen]byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		key := []byte(slot)
		existing := b.Get(key)
		// bbolt's Get returns a slice owned by the mmap — copy before mutation.
		next := make([]byte, 0, len(existing)+idLen)
		next = append(next, existing...)
		next = append(next, id[:]...)
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
func (s *Store) Range(fn func(slot string, ids [][idLen]byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			ids, err := decode(v)
			if err != nil {
				return fmt.Errorf("slot %s: %w", string(k), err)
			}
			return fn(string(k), ids)
		})
	})
}

// decode unpacks a slot value into a slice of fixed-size UUIDs.
func decode(v []byte) ([][idLen]byte, error) {
	if len(v)%idLen != 0 {
		return nil, errors.New("slot value not aligned to 16 bytes")
	}
	out := make([][idLen]byte, 0, len(v)/idLen)
	for i := 0; i < len(v); i += idLen {
		var id [idLen]byte
		copy(id[:], v[i:i+idLen])
		out = append(out, id)
	}
	return out, nil
}
