package scheduler

import (
	"time"

	"go.uber.org/zap"
)

// Recover rebuilds the in-memory wheel from bbolt on pod startup.
//
// Any slot whose (mm, ss) is already past relative to `now` (within the active
// hour) is dropped from both wheel and store — those rows missed their tick
// and are reconciled by Phase 5. Future slots are restored verbatim. This is
// the recovery contract from LLD §Scheduler.
func Recover(lg *zap.Logger, w *Wheel, s WheelStore, now time.Time) error {
	nowMM, nowSS := now.Minute(), now.Second()
	stale := 0
	restored := 0
	restoredIDs := 0
	var staleSlots []string

	err := s.Range(func(slot string, ids [][16]byte) error {
		mm, ss, ok := parseSlot(slot)
		if !ok {
			lg.Warn("recovery: bad slot key, skipping", zap.String("slot", slot))
			staleSlots = append(staleSlots, slot)
			return nil
		}
		// A slot is past-due if its (mm, ss) is strictly less than the current
		// (mm, ss). A slot at exactly the current second is also considered
		// past — by the time G3 next fires, the second has already advanced.
		if mm < nowMM || (mm == nowMM && ss <= nowSS) {
			staleSlots = append(staleSlots, slot)
			stale++
			return nil
		}
		for _, id := range ids {
			w.Append(mm, ss, id)
			restoredIDs++
		}
		restored++
		return nil
	})
	if err != nil {
		return err
	}

	for _, slot := range staleSlots {
		if err := s.Delete(slot); err != nil {
			lg.Warn("recovery: bbolt delete failed", zap.String("slot", slot), zap.Error(err))
		}
	}

	lg.Info("wheel recovery complete",
		zap.Int("slots_restored", restored),
		zap.Int("ids_restored", restoredIDs),
		zap.Int("slots_dropped_past_due", stale),
	)
	return nil
}

// parseSlot turns "MM:SS" into integers. Returns false if the input is
// malformed or out of range.
func parseSlot(s string) (mm, ss int, ok bool) {
	if len(s) != 5 || s[2] != ':' {
		return 0, 0, false
	}
	a, ok1 := atoi2(s[0], s[1])
	b, ok2 := atoi2(s[3], s[4])
	if !ok1 || !ok2 || a >= SlotsPerDim || b >= SlotsPerDim {
		return 0, 0, false
	}
	return a, b, true
}

func atoi2(hi, lo byte) (int, bool) {
	if hi < '0' || hi > '9' || lo < '0' || lo > '9' {
		return 0, false
	}
	return int(hi-'0')*10 + int(lo-'0'), true
}
