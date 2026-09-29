package scheduler

import (
	"path/filepath"
	"testing"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/db"
)

func openTestWheel(t *testing.T, path string) *wheel {
	t.Helper()
	w, err := openWheel(path)
	if err != nil {
		t.Fatalf("openWheel: %v", err)
	}
	t.Cleanup(func() { _ = w.close() })
	return w
}

func row(deliverAt time.Time) db.ListDueRow {
	id := uuid.New()
	return db.ListDueRow{ID: id[:], DeliverAt: deliverAt}
}

func dueIDs(t *testing.T, w *wheel, now time.Time) []uuid.UUID {
	t.Helper()
	keys, err := w.due(now)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	ids := make([]uuid.UUID, len(keys))
	for i, k := range keys {
		if ids[i], err = scheduleIDOf(k); err != nil {
			t.Fatalf("scheduleIDOf: %v", err)
		}
	}
	return ids
}

// A schedule must never fire before the time it was asked for, so a deliver_at
// with a fractional second fires in the next whole second.
func TestFireAtNeverRoundsDown(t *testing.T) {
	base := time.Date(2030, 1, 1, 12, 34, 56, 0, time.UTC)
	cases := []struct {
		deliverAt time.Time
		want      time.Time
	}{
		{base, base},
		{base.Add(time.Nanosecond), base.Add(time.Second)},
		{base.Add(999 * time.Millisecond), base.Add(time.Second)},
	}
	for _, c := range cases {
		if got := fireAt(c.deliverAt); got != c.want.Unix() {
			t.Errorf("fireAt(%s) = %s, want %s", c.deliverAt.Format(time.RFC3339Nano), time.Unix(got, 0).UTC(), c.want)
		}
	}
}

func TestWheelReturnsWhatIsDueInOrder(t *testing.T) {
	w := openTestWheel(t, filepath.Join(t.TempDir(), "wheel.db"))
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	later, soonest, due := row(now.Add(time.Second)), row(now.Add(-time.Minute)), row(now)
	if err := w.load([]db.ListDueRow{later, soonest, due}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	got := dueIDs(t, w, now)
	if len(got) != 2 || got[0] != uuid.UUID(soonest.ID) || got[1] != uuid.UUID(due.ID) {
		t.Fatalf("due = %v, want the two schedules due by now, earliest first", got)
	}
}

// Overlapping polls load the same schedule more than once; it must fire once.
func TestWheelHoldsARepeatedScheduleOnce(t *testing.T) {
	w := openTestWheel(t, filepath.Join(t.TempDir(), "wheel.db"))
	r := row(time.Now().Add(time.Minute))
	for range 2 {
		if err := w.load([]db.ListDueRow{r}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := w.size(); n != 1 {
		t.Fatalf("size = %d, want 1", n)
	}
}

// The point of keeping the wheel on disk: a restarted pod carries on with the
// schedules it had loaded, and knows how far ahead it had loaded them.
func TestWheelSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wheel.db")
	deliverAt := time.Now().Add(time.Minute)
	until := time.Now().Add(time.Hour)
	r := row(deliverAt)

	w, err := openWheel(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.load([]db.ListDueRow{r}, until); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	w = openTestWheel(t, path)
	if got := dueIDs(t, w, deliverAt.Add(time.Second)); len(got) != 1 || got[0] != uuid.UUID(r.ID) {
		t.Fatalf("after restart the wheel holds %v, want the schedule loaded before it", got)
	}
	if got, _ := w.loadedUntil(); !got.Equal(until) {
		t.Errorf("loadedUntil = %v, want %v", got, until)
	}
}

func TestWheelRemove(t *testing.T) {
	w := openTestWheel(t, filepath.Join(t.TempDir(), "wheel.db"))
	now := time.Now()
	if err := w.load([]db.ListDueRow{row(now.Add(-time.Second)), row(now.Add(-2 * time.Second))}, now); err != nil {
		t.Fatal(err)
	}
	keys, _ := w.due(now)
	if err := w.remove(keys[:1]); err != nil {
		t.Fatal(err)
	}
	if n, _ := w.size(); n != 1 {
		t.Fatalf("size = %d after removing one of two, want 1", n)
	}
}
