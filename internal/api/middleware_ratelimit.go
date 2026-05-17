package api

import (
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// rateLimitStore holds one limiter per client_id. Entries are never evicted
// during the pod lifetime — the working set is bounded by client count.
type rateLimitStore struct {
	m sync.Map // map[uuid.UUID]*rate.Limiter
}

func newRateLimitStore() *rateLimitStore { return &rateLimitStore{} }

func (s *rateLimitStore) limiterFor(id uuid.UUID, maxRPS int32) *rate.Limiter {
	if v, ok := s.m.Load(id); ok {
		return v.(*rate.Limiter)
	}
	l := rate.NewLimiter(rate.Limit(maxRPS), int(maxRPS)*2)
	actual, _ := s.m.LoadOrStore(id, l)
	return actual.(*rate.Limiter)
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
			l := store.limiterFor(id, rps)
			if !l.Allow() {
				mRateLimited.With(prometheus.Labels{"client_id": id.String()}).Inc()
				lg.Warn("Rate limited", zap.String("client_id", id.String()))
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusTooManyRequests, ErrCodeRateLimited, "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
