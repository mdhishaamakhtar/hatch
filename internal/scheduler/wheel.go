package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SlotsPerDim is the number of minute slots in an hour, and second slots in a
// minute. The wheel is a 60×60 array — one entry per (mm, ss) within the
// active hour. See LLD §Scheduler.
const SlotsPerDim = 60

// Slot is a position in the wheel: the minute and second within the active hour
// at which its schedules fire.
type Slot struct {
	Min, Sec int
}

// SlotOf is the slot whose firing window contains t — that is, the slot G3
// drains on the tick that lands inside t's second. The sub-second part of t is
// dropped, because the wheel's resolution is one second.
//
// This is the ticker's view. Use SlotForDeliverAt to place a schedule.
func SlotOf(t time.Time) Slot { return Slot{Min: t.Minute(), Sec: t.Second()} }

// SlotForDeliverAt is the slot a schedule due at deliverAt must be placed in.
//
// It rounds *up* to the next whole second whenever deliverAt carries a
// sub-second remainder. Placing it by SlotOf instead would truncate: a schedule
// for 12:34:56.789 would land in slot 34:56, which fires during the 56th second
// — up to 789ms *before* the caller asked for. A scheduler may run late; it must
// never deliver early, and "early" is the one error a caller cannot compensate
// for.
//
// The cost is that firing is now late by up to one second rather than early by
// up to one second. That is the wheel's resolution showing through, and it is
// the honest form of it: end-to-end latency measured from deliver_at is now
// always a real, non-negative duration.
func SlotForDeliverAt(deliverAt time.Time) Slot {
	whole := deliverAt.Truncate(time.Second)
	if whole.Before(deliverAt) {
		whole = whole.Add(time.Second)
	}
	return SlotOf(whole)
}

// String is the canonical "MM:SS" form, which doubles as the bbolt key.
func (s Slot) String() string { return fmt.Sprintf("%02d:%02d", s.Min, s.Sec) }

// valid reports whether both components are in range for the wheel array.
func (s Slot) valid() bool {
	return s.Min >= 0 && s.Min < SlotsPerDim && s.Sec >= 0 && s.Sec < SlotsPerDim
}

// ParseSlot turns "MM:SS" back into a Slot. ok is false if the input is
// malformed or out of range.
func ParseSlot(s string) (Slot, bool) {
	mmStr, ssStr, found := strings.Cut(s, ":")
	if !found {
		return Slot{}, false
	}
	mm, errMin := strconv.Atoi(mmStr)
	ss, errSec := strconv.Atoi(ssStr)
	if errMin != nil || errSec != nil {
		return Slot{}, false
	}
	slot := Slot{Min: mm, Sec: ss}
	return slot, slot.valid()
}

// SlotSummary is the JSON shape returned by /internal/wheel/slots.
type SlotSummary struct {
	Slot  string `json:"slot"`
	Count int    `json:"count"`
}

// Wheel is the in-memory timer wheel. G2 is the sole writer; G3 reads (and
// clears) entries on its 1-second tick. The mutex guards the slot slices —
// G2's bbolt write happens inside the same lock so memory and disk move
// together.
//
// present tracks every id currently in the wheel so one schedule can't be
// loaded twice. Recovery and the startup poll legitimately cover overlapping
// rows; without this, every restart would fire each of those rows twice.
type Wheel struct {
	mu      sync.Mutex
	slots   [SlotsPerDim][SlotsPerDim][][16]byte
	present map[[16]byte]struct{}
}

// NewWheel returns an empty wheel ready for use.
func NewWheel() *Wheel {
	return &Wheel{present: make(map[[16]byte]struct{})}
}

// Append adds id to the slot, reporting false if the wheel already holds it (a
// duplicate load, which the caller should neither persist nor count again).
func (w *Wheel) Append(slot Slot, id [16]byte) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, dup := w.present[id]; dup {
		return false
	}
	w.present[id] = struct{}{}
	w.slots[slot.Min][slot.Sec] = append(w.slots[slot.Min][slot.Sec], id)
	return true
}

// Drain returns and clears every id in the slot. G3 calls this once per tick.
func (w *Wheel) Drain(slot Slot) [][16]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := w.slots[slot.Min][slot.Sec]
	w.slots[slot.Min][slot.Sec] = nil
	for _, id := range ids {
		delete(w.present, id)
	}
	return ids
}

// Stats returns (occupied_slots, total_loaded). Cheap O(3600); called on every
// admin request and once per second by the ticker to update gauges.
func (w *Wheel) Stats() (occupied, total int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for mm := range SlotsPerDim {
		for ss := range SlotsPerDim {
			if n := len(w.slots[mm][ss]); n > 0 {
				occupied++
				total += n
			}
		}
	}
	return occupied, total
}

// Slots returns one entry per occupied slot, sorted by minute then second.
func (w *Wheel) Slots() []SlotSummary {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []SlotSummary
	for mm := range SlotsPerDim {
		for ss := range SlotsPerDim {
			if n := len(w.slots[mm][ss]); n > 0 {
				out = append(out, SlotSummary{Slot: Slot{Min: mm, Sec: ss}.String(), Count: n})
			}
		}
	}
	return out
}

// Slot returns the UUID-stringified ids currently in a slot. Used by the
// /internal/wheel/slots/{mm}/{ss} admin endpoint; binary ids are never exposed.
func (w *Wheel) Slot(slot Slot) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := w.slots[slot.Min][slot.Sec]
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, uuid.UUID(id).String())
	}
	return out
}
