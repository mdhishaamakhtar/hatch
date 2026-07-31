package recon

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/gen"
)

// Store is the narrow Postgres surface ReconcileOnce needs — just the two
// reconciliation passes. *gen.Queries satisfies it; a fake satisfies it in tests.
type Store interface {
	ReconPass1FreshAttempt(ctx context.Context) ([]gen.ReconPass1FreshAttemptRow, error)
	ReconPass2OrphanedRetry(ctx context.Context) ([]gen.ReconPass2OrphanedRetryRow, error)
}

var _ Store = (*gen.Queries)(nil)
