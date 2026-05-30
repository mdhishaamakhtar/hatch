package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

// errCacheUnavailable means Redis could not be reached after retries. The
// processor leaves the row `processing` so reconciliation re-enqueues it.
var errCacheUnavailable = errors.New("client cache unavailable")

// redisBackoffs is the inter-attempt delay schedule for transient Redis errors
// (LLD: up to 3 attempts at 50/100/200ms). Shared by the cache and idempotency.
var redisBackoffs = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

// cachedProvider mirrors one active client_providers row. Credentials stays the
// encrypted Tink envelope; the router decrypts it only when building a real
// provider (mock ignores it entirely).
type cachedProvider struct {
	Vendor      string          `json:"vendor"`
	Credentials json.RawMessage `json:"credentials"`
}

// clientInfo is the cached per-client snapshot the processor needs.
type clientInfo struct {
	IsActive  bool             `json:"is_active"`
	Providers []cachedProvider `json:"providers"`
}

// clientCache is the read-through Redis cache keyed `client:{uuid}` (the same
// key the API invalidates on client/provider mutation).
type clientCache struct {
	rc    rueidis.Client
	store Store
	ttl   time.Duration
	lg    *zap.Logger
}

func NewClientCache(rc rueidis.Client, store Store, ttl time.Duration, lg *zap.Logger) *clientCache {
	return &clientCache{rc: rc, store: store, ttl: ttl, lg: lg}
}

// cacheKey reproduces internal/api.clientCacheKey for a client id in byte form.
func cacheKey(clientID []byte) string {
	if u, err := db.BytesToUUID(clientID); err == nil {
		return "client:" + u.String()
	}
	return "client:invalid"
}

// Get returns the client snapshot, populating Redis on a miss. A nil error with
// a usable clientInfo is the only success; errCacheUnavailable means Redis was
// unreachable, and any other error is a Postgres failure — both leave the row
// untouched so it can be retried.
func (c *clientCache) Get(ctx context.Context, clientID []byte) (clientInfo, error) {
	key := cacheKey(clientID)

	raw, found, err := c.redisGet(ctx, key)
	if err != nil {
		mCacheOps.WithLabelValues("unavailable").Inc()
		return clientInfo{}, errCacheUnavailable
	}
	if found {
		var info clientInfo
		if jsonErr := json.Unmarshal(raw, &info); jsonErr == nil {
			mCacheOps.WithLabelValues("hit").Inc()
			return info, nil
		}
		c.lg.Warn("corrupt client cache value; reloading from Postgres", zap.String("key", key))
	}

	info, err := c.loadFromDB(ctx, clientID)
	if err != nil {
		return clientInfo{}, err
	}
	c.redisSet(ctx, key, info) // best-effort; a write failure just means the next read misses again
	mCacheOps.WithLabelValues("miss").Inc()
	return info, nil
}

// redisGet returns (value, found, err). A Redis Nil reply is found=false with a
// nil error; connection errors are retried before surfacing.
func (c *clientCache) redisGet(ctx context.Context, key string) ([]byte, bool, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 && !sleep(ctx, redisBackoffs[attempt-1]) {
			return nil, false, ctx.Err()
		}
		resp := c.rc.Do(ctx, c.rc.B().Get().Key(key).Build())
		if err := resp.Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				return nil, false, nil
			}
			lastErr = err
			continue
		}
		b, err := resp.AsBytes()
		if err != nil {
			lastErr = err
			continue
		}
		return b, true, nil
	}
	return nil, false, lastErr
}

func (c *clientCache) redisSet(ctx context.Context, key string, info clientInfo) {
	b, err := json.Marshal(info)
	if err != nil {
		return
	}
	cmd := c.rc.B().Set().Key(key).Value(rueidis.BinaryString(b)).
		ExSeconds(int64(c.ttl.Seconds())).Build()
	if err := c.rc.Do(ctx, cmd).Error(); err != nil {
		c.lg.Warn("client cache write failed", zap.String("key", key), zap.Error(err))
	}
}

func (c *clientCache) loadFromDB(ctx context.Context, clientID []byte) (clientInfo, error) {
	active, err := c.store.GetClientForDelivery(ctx, clientID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Unknown client → treat as inactive so the send is cancelled, not retried.
			return clientInfo{IsActive: false}, nil
		}
		return clientInfo{}, err
	}
	provs, err := c.store.ListClientActiveProviders(ctx, clientID)
	if err != nil {
		return clientInfo{}, err
	}
	info := clientInfo{IsActive: active, Providers: make([]cachedProvider, 0, len(provs))}
	for _, p := range provs {
		info.Providers = append(info.Providers, cachedProvider{
			Vendor:      p.Vendor,
			Credentials: json.RawMessage(p.Credentials),
		})
	}
	return info, nil
}

// sleep waits d or returns false if ctx is cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
