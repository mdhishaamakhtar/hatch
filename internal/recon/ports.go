package recon

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Store is the narrow Postgres surface ReconcileOnce needs — just the two
// reconciliation passes. *gen.Queries satisfies it; a fake satisfies it in tests.
type Store interface {
	ReconPass1FreshAttempt(ctx context.Context) ([]gen.ReconPass1FreshAttemptRow, error)
	ReconPass2OrphanedRetry(ctx context.Context) ([]gen.ReconPass2OrphanedRetryRow, error)
}

var _ Store = (*gen.Queries)(nil)

// Row aliases keep reconcile.go decoupled from the generated package name.
type (
	reconPass1Row = gen.ReconPass1FreshAttemptRow
	reconPass2Row = gen.ReconPass2OrphanedRetryRow
)

// Producer is the narrow Kafka produce surface the re-enqueue path needs. The
// synchronous signature lets the sweep handle produce errors per record. Mirrors
// internal/retry.Producer.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record) error
}

// kgoProducer adapts *kgo.Client to Producer so the sweep stays decoupled from
// franz-go specifics (and is faked in tests).
type kgoProducer struct{ cl *kgo.Client }

// NewKgoProducer wraps a franz-go client so it satisfies Producer.
func NewKgoProducer(cl *kgo.Client) Producer { return kgoProducer{cl: cl} }

func (k kgoProducer) Produce(ctx context.Context, r *kgo.Record) error {
	return k.cl.ProduceSync(ctx, r).FirstErr()
}
