package db

import (
	"time"
	"uuid"
)

// NewScheduleID returns a UUIDv7 stamped with deliverAt instead of the current
// time. scheduled_emails is partitioned by deliver_at, and a query that filters
// on id alone has to probe every one of its partitions; because deliver_at can
// be read back out of the id (ScheduleDeliverAt), every lookup by id can name
// its partition too. deliverAt is kept to the millisecond, the resolution the
// API accepts it in.
func NewScheduleID(deliverAt time.Time) uuid.UUID {
	id := uuid.NewV7()
	// A UUIDv7 starts with its timestamp: 48 bits of Unix milliseconds.
	ms := deliverAt.UnixMilli()
	for i := range 6 {
		id[i] = byte(ms >> (40 - 8*i))
	}
	return id
}

// ScheduleDeliverAt returns the deliver_at a schedule id was created for.
func ScheduleDeliverAt(id uuid.UUID) time.Time {
	var ms int64
	for _, b := range id[:6] {
		ms = ms<<8 | int64(b)
	}
	return time.UnixMilli(ms)
}
