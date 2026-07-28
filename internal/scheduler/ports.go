package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
)

// SchedulePoller is the narrow Postgres surface G1 needs. *gen.Queries
// satisfies it; tests use a fake.
type SchedulePoller interface {
	PollHourWindow(ctx context.Context, arg gen.PollHourWindowParams) ([]gen.PollHourWindowRow, error)
}

// MessageProducer is the narrow Kafka surface G3 needs. The synchronous
// signature lets the ticker record per-message produce latency directly.
type MessageProducer = kafka.Producer

// WheelStore is the narrow bbolt surface G2 + recovery need. *wheelstore.Store
// satisfies it; tests can use a fake or a tempdir-backed real store.
type WheelStore interface {
	Append(slot string, id [16]byte, deliverAt time.Time) error
	Delete(slot string) error
	Range(fn func(slot string, entries []wheelstore.Entry) error) error
}

// Compile-time interface checks.
var (
	_ WheelStore     = (*wheelstore.Store)(nil)
	_ SchedulePoller = (*gen.Queries)(nil)
)

// uuidString renders a [16]byte id in canonical UUID form. Centralised so the
// scheduler never leaks raw binary ids in logs, spans, JSON, or Kafka payloads.
func uuidString(id [16]byte) string { return uuid.UUID(id).String() }
