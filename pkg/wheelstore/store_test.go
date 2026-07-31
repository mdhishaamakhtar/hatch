package wheelstore

import (
	"path/filepath"
	"testing"
	"time"
)

func makeID(b byte) [idLen]byte {
	var id [idLen]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "wheel.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// collect drains the store into a map for assertion.
func collect(t *testing.T, s *Store) map[string][]Entry {
	t.Helper()
	got := map[string][]Entry{}
	if err := s.Range(func(slot string, entries []Entry) error {
		got[slot] = entries
		return nil
	}); err != nil {
		t.Fatalf("Range: %v", err)
	}
	return got
}

func TestAppendPreservesOrderAndDeliverAt(t *testing.T) {
	s := openTemp(t)
	// Truncated to the second: that's the resolution the packed value stores.
	base := time.Now().Truncate(time.Second)

	if err := s.Append("00:01", makeID(1), base); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i, b := range []byte{2, 3, 4} {
		if err := s.Append("32:47", makeID(b), base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got := collect(t, s)
	if len(got["00:01"]) != 1 || got["00:01"][0].ID != makeID(1) {
		t.Fatalf("slot 00:01 = %v", got["00:01"])
	}
	slot := got["32:47"]
	if len(slot) != 3 {
		t.Fatalf("slot 32:47 has %d entries, want 3", len(slot))
	}
	for i, b := range []byte{2, 3, 4} {
		if slot[i].ID != makeID(b) {
			t.Errorf("entry %d id = %x, want %x", i, slot[i].ID, makeID(b))
		}
		if want := base.Add(time.Duration(i) * time.Minute); !slot[i].DeliverAt.Equal(want) {
			t.Errorf("entry %d deliver_at = %v, want %v", i, slot[i].DeliverAt, want)
		}
	}
}

func TestDeleteRemovesTheWholeSlot(t *testing.T) {
	s := openTemp(t)
	if err := s.Append("11:11", makeID(7), time.Now()); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Delete("11:11"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := collect(t, s); len(got) != 0 {
		t.Fatalf("expected an empty store after Delete, got %v", got)
	}
}

// The whole point of the store: survive a pod restart.
func TestValuesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wheel.db")
	deliverAt := time.Now().Add(time.Hour).Truncate(time.Second)

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append("05:05", makeID(9), deliverAt); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer reopened.Close()

	got := collect(t, reopened)["05:05"]
	if len(got) != 1 || got[0].ID != makeID(9) || !got[0].DeliverAt.Equal(deliverAt) {
		t.Fatalf("after reopen got %v, want id 9 due %v", got, deliverAt)
	}
}

func TestDecodeRejectsMisalignedValue(t *testing.T) {
	if _, err := decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for a value that isn't a whole number of entries")
	}
}
