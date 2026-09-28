package db

import (
	"testing"
	"time"
)

func TestScheduleIDCarriesDeliverAt(t *testing.T) {
	deliverAt := time.Date(2031, 7, 4, 9, 30, 15, 123_000_000, time.UTC)

	id := NewScheduleID(deliverAt)
	if got := ScheduleDeliverAt(id); !got.Equal(deliverAt) {
		t.Errorf("ScheduleDeliverAt = %v, want %v", got, deliverAt)
	}
	if id.Version() != 7 {
		t.Errorf("version = %d, want a UUIDv7", id.Version())
	}
}

func TestScheduleIDsForTheSameInstantAreDistinct(t *testing.T) {
	deliverAt := time.Now()
	a, b := NewScheduleID(deliverAt), NewScheduleID(deliverAt)
	if a == b {
		t.Fatal("two schedules due at the same instant got the same id")
	}
}
