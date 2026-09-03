package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestWheelAppendDrainRoundTrip(t *testing.T) {
	w := NewWheel()
	slot := Slot{Min: 3, Sec: 7}
	w.Append(slot, id(1))
	w.Append(slot, id(2))

	got := w.Drain(slot)
	if len(got) != 2 || got[0] != id(1) || got[1] != id(2) {
		t.Fatalf("Drain = %v, want ids 1 and 2 in order", got)
	}
	if got := w.Drain(slot); len(got) != 0 {
		t.Fatalf("a drained slot should be empty, got %d ids", len(got))
	}
}

// Recovery and the startup poll cover overlapping rows, so the same id is
// offered to the wheel twice on every restart. Without this dedupe each of
// those rows would fire — and be sent — twice.
func TestWheelRejectsDuplicateID(t *testing.T) {
	w := NewWheel()
	slot := Slot{Min: 1, Sec: 1}

	if !w.Append(slot, id(9)) {
		t.Fatal("first append should be accepted")
	}
	if w.Append(slot, id(9)) {
		t.Error("re-appending the same id must be rejected")
	}
	if w.Append(Slot{Min: 2, Sec: 2}, id(9)) {
		t.Error("the same id must be rejected even in a different slot")
	}
	if _, total := w.Stats(); total != 1 {
		t.Fatalf("wheel holds %d ids, want 1", total)
	}
}

// Draining a slot releases its ids, so a later poll may legitimately load the
// same schedule again (e.g. after a reconciliation re-enqueue).
func TestWheelAllowsReAddAfterDrain(t *testing.T) {
	w := NewWheel()
	slot := Slot{Min: 4, Sec: 4}
	w.Append(slot, id(3))
	w.Drain(slot)
	if !w.Append(slot, id(3)) {
		t.Fatal("id should be loadable again once its slot has fired")
	}
}

func TestWheelStatsAndSlots(t *testing.T) {
	w := NewWheel()
	w.Append(Slot{Min: 0, Sec: 1}, id(1))
	w.Append(Slot{Min: 0, Sec: 1}, id(2))
	w.Append(Slot{Min: 5, Sec: 0}, id(3))

	occupied, total := w.Stats()
	if occupied != 2 || total != 3 {
		t.Fatalf("Stats = (%d, %d), want (2, 3)", occupied, total)
	}

	slots := w.Slots()
	if len(slots) != 2 || slots[0].Slot != "00:01" || slots[0].Count != 2 || slots[1].Slot != "05:00" {
		t.Fatalf("Slots = %+v, want 00:01 (2) then 05:00 (1)", slots)
	}
}

func TestWheelSlotRendersUUIDs(t *testing.T) {
	w := NewWheel()
	slot := Slot{Min: 9, Sec: 9}
	w.Append(slot, id(0xaa))
	got := w.Slot(slot)
	want := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("Slot = %v, want [%s]", got, want)
	}
}

func TestSlotStringAndParseRoundTrip(t *testing.T) {
	for _, s := range []Slot{{0, 0}, {3, 7}, {59, 59}} {
		key := s.String()
		back, ok := ParseSlot(key)
		if !ok || back != s {
			t.Errorf("ParseSlot(%q) = (%v, %v), want (%v, true)", key, back, ok, s)
		}
	}
}

func TestParseSlotRejectsBadInput(t *testing.T) {
	for _, bad := range []string{"", "bad", "3:7:9", "60:00", "00:60", "-1:00", "aa:bb", "0007"} {
		if _, ok := ParseSlot(bad); ok {
			t.Errorf("ParseSlot(%q) should have failed", bad)
		}
	}
}

func TestSlotOfUsesMinuteAndSecond(t *testing.T) {
	got := SlotOf(time.Date(2030, 6, 1, 13, 42, 17, 0, time.UTC))
	if want := (Slot{Min: 42, Sec: 17}); got != want {
		t.Fatalf("SlotOf = %v, want %v", got, want)
	}
}

// The wheel is written by G2 and read by G3 and the admin handlers, so its lock
// has to hold up underconcurrency.
func TestWheelIsSafeForConcurrentUse(t *testing.T) {
	const writers, perWriter = 8, 200
	w := NewWheel()

	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				n := writer*perWriter + i
				w.Append(Slot{Min: n % SlotsPerDim, Sec: (n / SlotsPerDim) % SlotsPerDim}, id(byte(n)))
			}
		}()
	}
	// Read concurrently to shake out lock inversions.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			w.Stats()
			w.Slots()
		}
	}()
	wg.Wait()

	// Only 256 distinct ids exist for 1600 appends, and duplicates are rejected.
	if _, total := w.Stats(); total != 256 {
		t.Fatalf("wheel holds %d distinct ids, want 256", total)
	}
}

// A schedule must never fire before the time the caller asked for. The wheel's
// resolution is one second, so a deliver_at carrying a sub-second remainder has
// to round up into the next slot — truncating would fire it up to 999ms early.
func TestSlotForDeliverAtNeverRoundsDown(t *testing.T) {
	base := time.Date(2030, 1, 1, 12, 34, 56, 0, time.UTC)

	cases := []struct {
		name      string
		deliverAt time.Time
		want      Slot
	}{
		{"exactly on the second stays put", base, Slot{Min: 34, Sec: 56}},
		{"1ns past rounds up", base.Add(time.Nanosecond), Slot{Min: 34, Sec: 57}},
		{"mid-second rounds up", base.Add(500 * time.Millisecond), Slot{Min: 34, Sec: 57}},
		{"999ms rounds up", base.Add(999 * time.Millisecond), Slot{Min: 34, Sec: 57}},
		{"rounds up across a minute", time.Date(2030, 1, 1, 12, 34, 59, 1, time.UTC), Slot{Min: 35, Sec: 0}},
		{"rounds up across an hour", time.Date(2030, 1, 1, 12, 59, 59, 1, time.UTC), Slot{Min: 0, Sec: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SlotForDeliverAt(tc.deliverAt); got != tc.want {
				t.Errorf("SlotForDeliverAt(%s) = %v, want %v", tc.deliverAt.Format(time.RFC3339Nano), got, tc.want)
			}
		})
	}
}

// The two slot functions serve opposite roles and must not be interchanged:
// SlotOf answers "which slot does this tick drain", SlotForDeliverAt answers
// "which slot must this schedule wait in". They agree only on whole seconds.
func TestSlotOfTruncatesWhereSlotForDeliverAtRoundsUp(t *testing.T) {
	mid := time.Date(2030, 1, 1, 12, 34, 56, int(500*time.Millisecond), time.UTC)

	if got, want := SlotOf(mid), (Slot{Min: 34, Sec: 56}); got != want {
		t.Errorf("SlotOf(%v) = %v, want %v — the ticker must drain the slot it is inside", mid, got, want)
	}
	if got, want := SlotForDeliverAt(mid), (Slot{Min: 34, Sec: 57}); got != want {
		t.Errorf("SlotForDeliverAt(%v) = %v, want %v — a schedule must not fire early", mid, got, want)
	}
}
