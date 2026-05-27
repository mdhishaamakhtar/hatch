package scheduler

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// SlotsPerDim is the number of minute slots in an hour, and second slots in a
// minute. The wheel is a 60×60 array — one entry per (mm, ss) within the
// active hour. See LLD §Scheduler.
const SlotsPerDim = 60

// SlotSummary is the JSON shape returned by /internal/wheel/slots.
type SlotSummary struct {
	Slot  string `json:"slot"`
	Count int    `json:"count"`
}

// Wheel is the in-memory timer wheel. G2 is the sole writer; G3 reads (and
// clears) entries on its 1-second tick. The mutex guards the slot slices —
// G2's bbolt write happens inside the same lock so memory and disk move
// together.
type Wheel struct {
	mu    sync.Mutex
	slots [SlotsPerDim][SlotsPerDim][][16]byte
}

// NewWheel returns an empty wheel ready for use.
func NewWheel() *Wheel { return &Wheel{} }

// Append adds id to the (mm, ss) slot.
func (w *Wheel) Append(mm, ss int, id [16]byte) {
	w.mu.Lock()
	w.slots[mm][ss] = append(w.slots[mm][ss], id)
	w.mu.Unlock()
}

// Drain returns and clears every id in the (mm, ss) slot. G3 calls this once
// per tick.
func (w *Wheel) Drain(mm, ss int) [][16]byte {
	w.mu.Lock()
	ids := w.slots[mm][ss]
	w.slots[mm][ss] = nil
	w.mu.Unlock()
	return ids
}

// Stats returns (occupied_slots, total_loaded). Cheap O(3600); called on every
// admin request and once per second by the ticker to update gauges.
func (w *Wheel) Stats() (occupied, total int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for mm := 0; mm < SlotsPerDim; mm++ {
		for ss := 0; ss < SlotsPerDim; ss++ {
			if n := len(w.slots[mm][ss]); n > 0 {
				occupied++
				total += n
			}
		}
	}
	return
}

// Slots returns one entry per occupied (mm, ss) slot, sorted by mm then ss.
func (w *Wheel) Slots() []SlotSummary {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []SlotSummary
	for mm := 0; mm < SlotsPerDim; mm++ {
		for ss := 0; ss < SlotsPerDim; ss++ {
			if n := len(w.slots[mm][ss]); n > 0 {
				out = append(out, SlotSummary{Slot: SlotKey(mm, ss), Count: n})
			}
		}
	}
	return out
}

// Slot returns the UUID-stringified ids currently in (mm, ss). Used by the
// /internal/wheel/slots/:mm/:ss admin endpoint; binary UUIDs are never exposed.
func (w *Wheel) Slot(mm, ss int) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := w.slots[mm][ss]
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		u := uuid.UUID(id)
		out = append(out, u.String())
	}
	return out
}

// SlotKey formats a (mm, ss) pair as the canonical "MM:SS" bbolt key.
func SlotKey(mm, ss int) string { return fmt.Sprintf("%02d:%02d", mm, ss) }
