package delivery

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Store is the narrow Postgres surface the batch processor needs. *gen.Queries
// satisfies it; tests use a fake.
type Store interface {
	BatchFetchSchedules(ctx context.Context, ids [][]byte) ([]gen.ScheduledEmail, error)
	MarkProcessing(ctx context.Context, arg gen.MarkProcessingParams) error
	MarkDelivered(ctx context.Context, arg gen.MarkDeliveredParams) error
	MarkRetrying(ctx context.Context, arg gen.MarkRetryingParams) error
	MarkFailed(ctx context.Context, arg gen.MarkFailedParams) error
	MarkCancelled(ctx context.Context, arg gen.MarkCancelledParams) error
	GetClientForDelivery(ctx context.Context, id []byte) (bool, error)
	ListClientActiveProviders(ctx context.Context, clientID []byte) ([]gen.ListClientActiveProvidersRow, error)
}

// Producer is the narrow Kafka produce surface the retry path needs. The
// synchronous signature lets callers handle produce errors inline.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record) error
}

// kgoProducer adapts *kgo.Client to Producer so the retry path stays decoupled
// from franz-go specifics. Mirrors internal/scheduler.NewKgoProducer.
type kgoProducer struct{ cl *kgo.Client }

// NewKgoProducer wraps a franz-go client so it satisfies Producer.
func NewKgoProducer(cl *kgo.Client) Producer { return kgoProducer{cl: cl} }

func (k kgoProducer) Produce(ctx context.Context, r *kgo.Record) error {
	return k.cl.ProduceSync(ctx, r).FirstErr()
}

// Compile-time check.
var _ Store = (*gen.Queries)(nil)
