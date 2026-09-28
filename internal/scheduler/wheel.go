package scheduler

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	bolt "go.etcd.io/bbolt"
)

var (
	// schedulesBucket holds one key per loaded schedule: the second it fires
	// (Unix time, big-endian) followed by its id. bbolt keeps keys sorted, so a
	// cursor walks schedules in the order they fall due, and loading the same
	// schedule twice writes the same key.
	schedulesBucket = []byte("schedules")

	// metaBucket holds loadedUntilKey: how far ahead the wheel has been loaded.
	metaBucket     = []byte("meta")
	loadedUntilKey = []byte("loaded_until")
)

// wheel is the scheduler's timer: the schedules this pod has loaded and not yet
// fired, kept in bbolt so that they survive a restart. A restarted pod carries
// on from exactly where it stopped, and whatever came due while it was down
// fires on its first tick.
type wheel struct {
	db *bolt.DB
}

func openWheel(path string) (*wheel, error) {
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	err = bdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(schedulesBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	})
	if err != nil {
		bdb.Close()
		return nil, err
	}
	return &wheel{db: bdb}, nil
}

func (w *wheel) close() error { return w.db.Close() }

// load adds schedules to the wheel and records that everything due up to until
// has now been loaded, in one transaction.
func (w *wheel) load(rows []db.ListDueRow, until time.Time) error {
	untilBytes, err := until.MarshalBinary()
	if err != nil {
		return err
	}
	return w.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schedulesBucket)
		for _, row := range rows {
			id, err := uuid.FromBytes(row.ID)
			if err != nil {
				return err
			}
			if err := b.Put(wheelKey(fireAt(row.DeliverAt), id), nil); err != nil {
				return err
			}
		}
		return tx.Bucket(metaBucket).Put(loadedUntilKey, untilBytes)
	})
}

// loadedUntil reports how far ahead the wheel has been loaded, or the zero time
// if it never has been.
func (w *wheel) loadedUntil() (time.Time, error) {
	var until time.Time
	err := w.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(metaBucket).Get(loadedUntilKey)
		if v == nil {
			return nil
		}
		return until.UnmarshalBinary(v)
	})
	return until, err
}

// due returns the key of every schedule that fires at or before now.
func (w *wheel) due(now time.Time) ([][]byte, error) {
	limit := wheelKey(now.Unix()+1, uuid.Nil)
	var keys [][]byte
	err := w.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(schedulesBucket).Cursor()
		for k, _ := c.First(); k != nil && bytes.Compare(k, limit) < 0; k, _ = c.Next() {
			// bbolt's keys are only valid inside the transaction.
			keys = append(keys, bytes.Clone(k))
		}
		return nil
	})
	return keys, err
}

// remove deletes schedules by key once they have been fired.
func (w *wheel) remove(keys [][]byte) error {
	return w.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schedulesBucket)
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// size returns how many schedules the wheel holds.
func (w *wheel) size() (int, error) {
	var n int
	err := w.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(schedulesBucket).Stats().KeyN
		return nil
	})
	return n, err
}

// fireAt is the Unix second a schedule due at deliverAt fires in. It rounds up:
// the wheel ticks once a second, and a schedule may fire late but never early.
func fireAt(deliverAt time.Time) int64 {
	sec := deliverAt.Unix()
	if deliverAt.Nanosecond() > 0 {
		sec++
	}
	return sec
}

func wheelKey(fireAt int64, id uuid.UUID) []byte {
	return append(binary.BigEndian.AppendUint64(nil, uint64(fireAt)), id[:]...)
}

func scheduleIDOf(key []byte) (uuid.UUID, error) {
	if len(key) != 8+16 {
		return uuid.Nil, errors.New("malformed wheel key")
	}
	return uuid.FromBytes(key[8:])
}
