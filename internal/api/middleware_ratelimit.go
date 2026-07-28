package api

import (
	"net/http"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// clientLimiter is one client's limiter plus the max_rps it was built for, so a
// later request carrying a different rate retunes it in place. Caching the
// limiter alone would mean an admin's rate change never took effect until the
// API pods restarted.
type clientLimiter struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	rps     int32
}

// allow consumes a token, first retuning the limiter if max_rps has changed.
func (c *clientLimiter) allow(rps int32) bool {
	c.mu.Lock()
	if rps != c.rps {
		c.rps = rps
		c.limiter.SetLimit(rate.Limit(rps))
		c.limiter.SetBurst(burstFor(rps))
	}
	limiter := c.limiter
	c.mu.Unlock()
	return limiter.Allow()
}

// burstFor allows a one-second burst at double the steady rate.
func burstFor(rps int32) int { return int(rps) * 2 }

// rateLimitStore holds one limiter per client_id. Entries are never evicted
// during the pod lifetime — the working set is bounded by client count.
type rateLimitStore struct {
	m sync.Map // map[uuid.UUID]*clientLimiter
}

func newRateLimitStore() *rateLimitStore { return &rateLimitStore{} }

func (s *rateLimitStore) limiterFor(id uuid.UUID, maxRPS int32) *clientLimiter {
	if v, ok := s.m.Load(id); ok {
		return v.(*clientLimiter)
	}
	fresh := &clientLimiter{limiter: rate.NewLimiter(rate.Limit(maxRPS), burstFor(maxRPS)), rps: maxRPS}
	actual, _ := s.m.LoadOrStore(id, fresh)
	return actual.(*clientLimiter)
}

// RateLimit enforces per-client RPS limits. Must run after ClientAuth so
// (client_id, max_rps) is in ctx.
func RateLimit(store *rateLimitStore, lg *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, ok := ClientIDFromCtx(r.Context())
			if !ok {
				// No auth context — let it through; an upstream 401 already happened.
				next.ServeHTTP(w, r)
				return
			}
			rps, _ := maxRPSFromCtx(r.Context())
			if rps <= 0 {
				rps = 1
			}
			if !store.limiterFor(id, rps).allow(rps) {
				mRateLimited.Inc()
				lg.Warn("Rate limited", zap.String("client_id", id.String()))
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusTooManyRequests, ErrCodeRateLimited, "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
