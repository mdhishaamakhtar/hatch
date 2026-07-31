package scheduler

import (
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
	"go.uber.org/zap"
)

// Recover rebuilds the in-memory wheel from bbolt on pod startup.
//
// An entry is restored only if its deliver_at is still in the future; anything
// already due is dropped from both wheel and store, because it missed its tick
// and reconciliation owns it now. Comparing the stored deliver_at (rather than
// the "MM:SS" key, which carries no hour) is what stops a pod that was down
// across an hour boundary from restoring hour-old slots as "future" and firing
// them up to 59 minutes late.
//
// The startup poll runs right after this over an overlapping window, so some ids
// get loaded twice; the wheel's duplicate check absorbs that.
func Recover(lg *zap.Logger, w *Wheel, s WheelStore, now time.Time) error {
	var (
		restoredSlots, restoredIDs, staleEntries int
		deadSlots                                []string
	)

	err := s.Range(func(key string, entries []wheelstore.Entry) error {
		slot, ok := ParseSlot(key)
		if !ok {
			lg.Warn("recovery: bad slot key, skipping", zap.String("slot", key))
			deadSlots = append(deadSlots, key)
			return nil
		}
		restored := 0
		for _, e := range entries {
			if !e.DeliverAt.After(now) {
				staleEntries++
				continue
			}
			if w.Append(slot, e.ID) {
				restoredIDs++
				restored++
			}
		}
		if restored == 0 {
			// Nothing in this slot survived — drop the key rather than leave a
			// tombstone every future recovery re-reads.
			deadSlots = append(deadSlots, key)
			return nil
		}
		restoredSlots++
		return nil
	})
	if err != nil {
		return err
	}

	for _, key := range deadSlots {
		if err := s.Delete(key); err != nil {
			lg.Warn("recovery: bbolt delete failed", zap.String("slot", key), zap.Error(err))
		}
	}

	lg.Info("wheel recovery complete",
		zap.Int("slots_restored", restoredSlots),
		zap.Int("ids_restored", restoredIDs),
		zap.Int("entries_dropped_past_due", staleEntries),
		zap.Int("slots_dropped", len(deadSlots)),
	)
	return nil
}
