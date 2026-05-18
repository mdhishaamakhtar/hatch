package api

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/rueidis"
)

// healthHandler always returns 200 — liveness only checks the process.
//
//	@Summary	Liveness probe
//	@Tags		health
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Router		/healthz [get]
func healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok here!"}`))
	}
}

// readyHandler pings Postgres and Redis with a tight deadline. A failure on
// either flips readiness to 503 so k8s removes the pod from the service
// endpoints — the process keeps running so its /metrics endpoint stays
// scrapable through the outage.
//
//	@Summary	Readiness probe (pings Postgres + Redis)
//	@Tags		health
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Failure	503	{object}	apiError
//	@Router		/readyz [get]
func readyHandler(pool *pgxpool.Pool, rc rueidis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "postgres")
			return
		}
		if err := rc.Do(ctx, rc.B().Ping().Build()).Error(); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "redis")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}
}
