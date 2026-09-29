package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

// clientCacheTTL bounds how stale a cached client can get if an invalidation
// from the API is ever lost.
const clientCacheTTL = 5 * time.Minute

// client is what a send needs to know about the schedule's client.
type client struct {
	Active    bool             `json:"active"`
	Providers []cachedProvider `json:"providers"`
}

type cachedProvider struct {
	Vendor      string `json:"vendor"`
	Credentials []byte `json:"credentials"` // still sealed; see internal/crypto
}

// clientCache is a read-through cache of clients in Redis, under client:<id>.
// The API deletes that key whenever a client or its providers change.
type clientCache struct {
	redis   rueidis.Client
	queries *db.Queries
	lg      *zap.Logger
}

func (c *clientCache) lookup(ctx context.Context, clientID uuid.UUID) (client, error) {
	key := "client:" + clientID.String()
	raw, err := c.redis.Do(ctx, c.redis.B().Get().Key(key).Build()).AsBytes()
	switch {
	case err == nil:
		var cl client
		if json.Unmarshal(raw, &cl) == nil {
			cacheLookups.WithLabelValues("hit").Inc()
			return cl, nil
		}
		c.lg.Warn("unreadable client cache entry; reloading it", zap.String("key", key))
	case !rueidis.IsRedisNil(err):
		cacheLookups.WithLabelValues("unavailable").Inc()
		return client{}, err
	}

	cacheLookups.WithLabelValues("miss").Inc()
	cl, err := c.load(ctx, clientID)
	if err != nil {
		return client{}, err
	}
	raw, _ = json.Marshal(cl)
	if err := c.redis.Do(ctx, c.redis.B().Set().Key(key).Value(rueidis.BinaryString(raw)).Ex(clientCacheTTL).Build()).Error(); err != nil {
		c.lg.Warn("client cache write failed", zap.String("key", key), zap.Error(err))
	}
	return cl, nil
}

func (c *clientCache) load(ctx context.Context, clientID uuid.UUID) (client, error) {
	active, err := c.queries.IsClientActive(ctx, clientID[:])
	if errors.Is(err, pgx.ErrNoRows) {
		return client{}, nil // an unknown client sends nothing, like a deleted one
	}
	if err != nil {
		return client{}, err
	}
	rows, err := c.queries.ListActiveProviders(ctx, clientID[:])
	if err != nil {
		return client{}, err
	}
	cl := client{Active: active}
	for _, r := range rows {
		cl.Providers = append(cl.Providers, cachedProvider{Vendor: r.Vendor, Credentials: r.Credentials})
	}
	return cl, nil
}
