package retry

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is the narrow Kafka produce surface the re-enqueue path needs. The
// synchronous signature lets the drain loop handle produce errors inline and
// decide whether to commit. Mirrors internal/delivery.Producer.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record) error
}

// kgoProducer adapts *kgo.Client to Producer so the drain path stays decoupled
// from franz-go specifics (and is faked in tests).
type kgoProducer struct{ cl *kgo.Client }

// NewKgoProducer wraps a franz-go client so it satisfies Producer.
func NewKgoProducer(cl *kgo.Client) Producer { return kgoProducer{cl: cl} }

func (k kgoProducer) Produce(ctx context.Context, r *kgo.Record) error {
	return k.cl.ProduceSync(ctx, r).FirstErr()
}
