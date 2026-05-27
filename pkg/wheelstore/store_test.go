package wheelstore

import (
	"path/filepath"
	"testing"
)

func makeID(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wheel.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAppendThenRange(t *testing.T) {
	s := openTemp(t)
	want := []struct {
		slot string
		ids  [][16]byte
	}{
		{"00:01", [][16]byte{makeID(1)}},
		{"32:47", [][16]byte{makeID(2), makeID(3), makeID(4)}},
	}
	for _, w := range want {
		for _, id := range w.ids {
			if err := s.Append(w.slot, id); err != nil {
				t.Fatalf("Append(%s): %v", w.slot, err)
			}
		}
	}
	got := map[string][][16]byte{}
	if err := s.Range(func(slot string, ids [][16]byte) error {
		got[slot] = ids
		return nil
	}); err != nil {
		t.Fatalf("Range: %v", err)
	}
	for _, w := range want {
		gids := got[w.slot]
		if len(gids) != len(w.ids) {
			t.Fatalf("slot %s: got %d ids, want %d", w.slot, len(gids), len(w.ids))
		}
		for i, id := range w.ids {
			if gids[i] != id {
				t.Fatalf("slot %s [%d]: got %x want %x", w.slot, i, gids[i], id)
			}
		}
	}
}

func TestDelete(t *testing.T) {
	s := openTemp(t)
	if err := s.Append("11:11", makeID(7)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Delete("11:11"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	count := 0
	_ = s.Range(func(string, [][16]byte) error { count++; return nil })
	if count != 0 {
		t.Fatalf("expected 0 slots after Delete, got %d", count)
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wheel.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append("05:05", makeID(9)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close()

	var found [][16]byte
	_ = s2.Range(func(slot string, ids [][16]byte) error {
		if slot == "05:05" {
			found = ids
		}
		return nil
	})
	if len(found) != 1 || found[0] != makeID(9) {
		t.Fatalf("after reopen got %v, want one ID of all-9s", found)
	}
}

func TestDecodeRejectsMisalignedValue(t *testing.T) {
	if _, err := decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for non-16-byte-aligned slot value")
	}
}
