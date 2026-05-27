package scheduler

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SchedulePoller is the narrow Postgres surface G1 needs. *gen.Queries
// satisfies it; tests use a fake.
type SchedulePoller interface {
	PollHourWindow(ctx context.Context, arg gen.PollHourWindowParams) ([]gen.PollHourWindowRow, error)
}

// MessageProducer is the narrow Kafka surface G3 needs. The synchronous
// signature lets the ticker record per-message produce latency directly; the
// real implementation wraps *kgo.Client with kgo.Client.ProduceSync.
type MessageProducer interface {
	Produce(ctx context.Context, r *kgo.Record) error
}

// WheelStore is the narrow bbolt surface G2 + recovery need. *wheelstore.Store
// satisfies it; tests can use a fake or a tempdir-backed real store.
type WheelStore interface {
	Append(slot string, id [16]byte) error
	Delete(slot string) error
	Range(fn func(slot string, ids [][16]byte) error) error
}

// kgoProducer adapts *kgo.Client to the MessageProducer interface so the
// ticker code stays decoupled from franz-go specifics.
type kgoProducer struct{ cl *kgo.Client }

// NewKgoProducer wraps a franz-go client so it satisfies MessageProducer.
func NewKgoProducer(cl *kgo.Client) MessageProducer { return kgoProducer{cl: cl} }

func (k kgoProducer) Produce(ctx context.Context, r *kgo.Record) error {
	return k.cl.ProduceSync(ctx, r).FirstErr()
}

// Compile-time interface checks.
var (
	_ WheelStore     = (*wheelstore.Store)(nil)
	_ SchedulePoller = (*gen.Queries)(nil)
)
