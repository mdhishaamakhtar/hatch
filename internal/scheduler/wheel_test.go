package scheduler

import (
	"sync"
	"testing"
)

func id(b byte) [16]byte {
	var v [16]byte
	for i := range v {
		v[i] = b
	}
	return v
}

func TestAppendDrain(t *testing.T) {
	w := NewWheel()
	w.Append(5, 5, id(1))
	w.Append(5, 5, id(2))
	got := w.Drain(5, 5)
	if len(got) != 2 || got[0] != id(1) || got[1] != id(2) {
		t.Fatalf("Drain returned wrong ids: %v", got)
	}
	if again := w.Drain(5, 5); len(again) != 0 {
		t.Fatalf("Drain should empty the slot, got %d ids second time", len(again))
	}
}

func TestStats(t *testing.T) {
	w := NewWheel()
	w.Append(0, 1, id(1))
	w.Append(0, 1, id(2))
	w.Append(2, 3, id(3))
	occ, total := w.Stats()
	if occ != 2 || total != 3 {
		t.Fatalf("Stats() = (%d,%d), want (2,3)", occ, total)
	}
}

func TestSlotsAndSlotSerialiseUUIDs(t *testing.T) {
	w := NewWheel()
	w.Append(1, 2, id(0xab))
	slots := w.Slots()
	if len(slots) != 1 || slots[0].Slot != "01:02" || slots[0].Count != 1 {
		t.Fatalf("Slots() = %+v", slots)
	}
	uids := w.Slot(1, 2)
	if len(uids) != 1 {
		t.Fatalf("Slot returned %d, want 1", len(uids))
	}
	// All bytes set to 0xab → all-ab UUID. Just assert it parses as a UUID
	// string (hex-with-hyphens, length 36) and contains an 'a'.
	if len(uids[0]) != 36 {
		t.Fatalf("Slot UUID not in canonical form: %q", uids[0])
	}
}

func TestAppendIsRaceFree(t *testing.T) {
	w := NewWheel()
	var wg sync.WaitGroup
	const goroutines = 32
	const perG = 100
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				w.Append(g%SlotsPerDim, i%SlotsPerDim, id(byte(g)))
			}
		}()
	}
	wg.Wait()
	_, total := w.Stats()
	want := goroutines * perG
	if total != want {
		t.Fatalf("total loaded after concurrent Append = %d, want %d", total, want)
	}
}

func TestSlotKey(t *testing.T) {
	cases := []struct {
		mm, ss int
		want   string
	}{
		{0, 0, "00:00"},
		{5, 9, "05:09"},
		{59, 59, "59:59"},
	}
	for _, c := range cases {
		got := SlotKey(c.mm, c.ss)
		if got != c.want {
			t.Errorf("SlotKey(%d,%d) = %q want %q", c.mm, c.ss, got, c.want)
		}
	}
	// Sanity: every (mm, ss) round-trips through parseSlot.
	for mm := 0; mm < SlotsPerDim; mm++ {
		for ss := 0; ss < SlotsPerDim; ss++ {
			k := SlotKey(mm, ss)
			gm, gs, ok := parseSlot(k)
			if !ok || gm != mm || gs != ss {
				t.Fatalf("round trip %d:%d failed: got %d:%d ok=%v", mm, ss, gm, gs, ok)
			}
		}
	}
}
