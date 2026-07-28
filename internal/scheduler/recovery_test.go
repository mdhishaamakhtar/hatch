package scheduler

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRecoverRestoresFutureEntriesOnly(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 30, 30, 0, time.UTC)
	store := newFakeStore()
	_ = store.Append("30:29", id(1), now.Add(-time.Second)) // just missed
	_ = store.Append("30:30", id(2), now)                   // this very second — already gone
	_ = store.Append("30:31", id(3), now.Add(time.Second))  // still ahead
	_ = store.Append("45:00", id(4), now.Add(15*time.Minute))

	w := NewWheel()
	if err := Recover(zap.NewNop(), w, store, now); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if got := w.Drain(Slot{Min: 30, Sec: 31}); len(got) != 1 || got[0] != id(3) {
		t.Errorf("30:31 = %v, want id(3)", got)
	}
	if got := w.Drain(Slot{Min: 45, Sec: 0}); len(got) != 1 || got[0] != id(4) {
		t.Errorf("45:00 = %v, want id(4)", got)
	}

	deleted := map[string]bool{}
	for _, slot := range store.deleteLog() {
		deleted[slot] = true
	}
	for _, want := range []string{"30:29", "30:30"} {
		if !deleted[want] {
			t.Errorf("past-due slot %s should have been scrubbed from the store", want)
		}
	}
}

// The "MM:SS" key carries no hour, so a pod down across an hour boundary sees
// an hour-old slot as a future minute. The stored deliver_at is what stops it
// being restored and fired up to 59 minutes late.
func TestRecoverDropsEntriesFromAPreviousHour(t *testing.T) {
	now := time.Date(2030, 1, 1, 13, 5, 0, 0, time.UTC)
	staleButLaterInTheMinute := time.Date(2030, 1, 1, 12, 45, 0, 0, time.UTC)

	store := newFakeStore()
	_ = store.Append("45:00", id(1), staleButLaterInTheMinute)

	w := NewWheel()
	if err := Recover(zap.NewNop(), w, store, now); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if _, total := w.Stats(); total != 0 {
		t.Fatalf("an entry due in the previous hour must not be restored, wheel holds %d", total)
	}
	if _, present := store.snapshot()["45:00"]; present {
		t.Error("the stale slot should have been deleted from the store")
	}
}

func TestRecoverDeletesMalformedKeys(t *testing.T) {
	store := newFakeStore()
	_ = store.Append("bad", id(7), time.Now().Add(time.Hour))

	w := NewWheel()
	if err := Recover(zap.NewNop(), w, store, time.Now()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, present := store.snapshot()["bad"]; present {
		t.Fatal("a malformed key should be deleted rather than left to accumulate")
	}
}
