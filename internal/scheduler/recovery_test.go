package scheduler

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// fakeStore is an in-memory WheelStore for tests. It also records deletes so
// the test can assert which slots Recovery scrubbed. Thread-safe — RunBuilder
// runs it from its own goroutine while tests read from the test goroutine.
type fakeStore struct {
	mu      sync.Mutex
	data    map[string][][16]byte
	deletes []string
}

func newFakeStore() *fakeStore { return &fakeStore{data: map[string][][16]byte{}} }

func (f *fakeStore) Append(slot string, id [16]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[slot] = append(f.data[slot], id)
	return nil
}

func (f *fakeStore) Delete(slot string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, slot)
	f.deletes = append(f.deletes, slot)
	return nil
}

func (f *fakeStore) Range(fn func(string, [][16]byte) error) error {
	f.mu.Lock()
	snapshot := make(map[string][][16]byte, len(f.data))
	for k, v := range f.data {
		ids := make([][16]byte, len(v))
		copy(ids, v)
		snapshot[k] = ids
	}
	f.mu.Unlock()
	for k, v := range snapshot {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) deleteLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deletes))
	copy(out, f.deletes)
	return out
}

func (f *fakeStore) snapshotData() map[string][][16]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][][16]byte, len(f.data))
	for k, v := range f.data {
		ids := make([][16]byte, len(v))
		copy(ids, v)
		out[k] = ids
	}
	return out
}

func TestRecoverKeepsFutureDropsPast(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 30, 30, 0, time.UTC)
	s := newFakeStore()
	_ = s.Append("00:05", id(1)) // past (mm < nowMM)
	_ = s.Append("30:29", id(2)) // past (mm == nowMM, ss < nowSS)
	_ = s.Append("30:30", id(3)) // current second — treated as past per LLD
	_ = s.Append("30:31", id(4)) // future
	_ = s.Append("45:00", id(5)) // future
	s.deletes = nil

	w := NewWheel()
	if err := Recover(zap.NewNop(), w, s, now); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// 00:05, 30:29, 30:30 should have been deleted from the store.
	deletedSet := map[string]bool{}
	for _, k := range s.deleteLog() {
		deletedSet[k] = true
	}
	for _, want := range []string{"00:05", "30:29", "30:30"} {
		if !deletedSet[want] {
			t.Errorf("expected Recover to delete %s", want)
		}
	}

	// Future slots should be reloaded into the wheel.
	if got := w.Drain(30, 31); len(got) != 1 || got[0] != id(4) {
		t.Errorf("expected id(4) in 30:31, got %v", got)
	}
	if got := w.Drain(45, 0); len(got) != 1 || got[0] != id(5) {
		t.Errorf("expected id(5) in 45:00, got %v", got)
	}
}

func TestRecoverIgnoresMalformedKey(t *testing.T) {
	s := newFakeStore()
	s.data["bad"] = [][16]byte{id(7)}
	w := NewWheel()
	if err := Recover(zap.NewNop(), w, s, time.Now()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Malformed keys are deleted along with stale slots so they don't pile up.
	if _, present := s.snapshotData()["bad"]; present {
		t.Fatal("malformed key should have been deleted")
	}
}
