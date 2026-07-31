package delivery

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// Store is the narrow Postgres surface the batch processor needs. *gen.Queries
// satisfies it; tests use a fake.
//
// The MarkX calls return the number of rows they changed. 0 means the row moved
// to a state the transition isn't legal from (a concurrent cancel, or a
// duplicate emails.due record for an already-terminal row) — the caller logs and
// stops rather than overwriting a terminal state.
type Store interface {
	BatchFetchSchedules(ctx context.Context, ids [][]byte) ([]gen.ScheduledEmail, error)
	MarkProcessing(ctx context.Context, arg gen.MarkProcessingParams) (int64, error)
	MarkDelivered(ctx context.Context, arg gen.MarkDeliveredParams) (int64, error)
	MarkRetrying(ctx context.Context, arg gen.MarkRetryingParams) (int64, error)
	MarkFailed(ctx context.Context, arg gen.MarkFailedParams) (int64, error)
	MarkCancelled(ctx context.Context, arg gen.MarkCancelledParams) (int64, error)
	GetClientForDelivery(ctx context.Context, id []byte) (bool, error)
	ListClientActiveProviders(ctx context.Context, clientID []byte) ([]gen.ListClientActiveProvidersRow, error)
}

// Compile-time check.
var _ Store = (*gen.Queries)(nil)

// uuidString renders a 16-byte schedule/client id as its canonical UUID string.
// Returns "" if the bytes are not a valid UUID.
func uuidString(b []byte) string {
	u, err := db.BytesToUUID(b)
	if err != nil {
		return ""
	}
	return u.String()
}
