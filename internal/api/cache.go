package api

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

// clientCacheKey is the per-client Redis cache key the Delivery Worker reads.
// API never reads this — it only invalidates after mutations.
func clientCacheKey(id uuid.UUID) string {
	return "client:" + id.String()
}

// invalidateClientCache best-effort DELs the cache key. Errors are returned so
// callers can log WARN, but a Redis miss never fails the mutation.
func invalidateClientCache(ctx context.Context, rc rueidis.Client, id uuid.UUID) error {
	return rc.Do(ctx, rc.B().Del().Key(clientCacheKey(id)).Build()).Error()
}
