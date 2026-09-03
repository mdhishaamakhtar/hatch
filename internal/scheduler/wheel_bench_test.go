package scheduler

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// Sinks stop the compiler from eliminating the work under test.
var (
	sinkInt int
	sinkIDs [][16]byte
)

// benchID builds a distinct id per n. The package's id() test helper fills all
// 16 bytes with one value, so it only spans 256 ids — and the wheel rejects
// duplicates, which would silently under-fill every benchmark below.
func benchID(n int) [16]byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[:8], uint64(n))
	return out
}

// benchSlot spreads ids evenly over the wheel's 3600 slots, the way an hour of
// real deliver_at values would.
func benchSlot(n int) Slot {
	return Slot{Min: (n / SlotsPerDim) % SlotsPerDim, Sec: n % SlotsPerDim}
}

func fillWheel(n int) *Wheel {
	w := NewWheel()
	for i := range n {
		w.Append(benchSlot(i), benchID(i))
	}
	return w
}

// BenchmarkWheelLoad measures G2 loading a whole poll into the wheel — the unit
// that actually matters, since the poller hands over a full hour of work at
// once (SCHEDULER_SCHEDULE_CHANNEL_BUFFER defaults to 100,000).
//
// ns/op is per batch of n, so divide by n for the per-schedule cost.
func BenchmarkWheelLoad(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("schedules=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				w := NewWheel()
				for i := range n {
					w.Append(benchSlot(i), benchID(i))
				}
				sinkInt, _ = w.Stats()
			}
		})
	}
}

// BenchmarkWheelStats measures the O(3600) occupancy scan.
//
// This is the one worth watching. Stats() is called by publishWheelGauges, which
// the builder runs after *every* successful append — so loading a poll of N
// schedules costs N full 3600-slot scans, not one. Multiply this number by the
// poll size to get the hidden cost of keeping the gauges fresh.
//
// Occupancy is swept because the scan walks slots, not ids: a nearly-empty wheel
// covers the same 3600 entries as a full one.
func BenchmarkWheelStats(b *testing.B) {
	for _, loaded := range []int{0, 3_600, 100_000} {
		w := fillWheel(loaded)
		b.Run(fmt.Sprintf("loaded=%d", loaded), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				occupied, _ := w.Stats()
				sinkInt = occupied
			}
		})
	}
}

// BenchmarkWheelDrain measures the per-tick cost G3 pays to empty one slot.
// Drain also clears the ids from the dedupe set, so refilling with the same ids
// each round stays steady-state.
func BenchmarkWheelDrain(b *testing.B) {
	for _, perSlot := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("ids=%d", perSlot), func(b *testing.B) {
			w := NewWheel()
			slot := Slot{Min: 30, Sec: 30}
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for i := range perSlot {
					w.Append(slot, benchID(i))
				}
				b.StartTimer()
				sinkIDs = w.Drain(slot)
			}
		})
	}
}
