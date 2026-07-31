package delivery

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"github.com/redis/rueidis"
)

// AdminHandler is the delivery worker's health/observability HTTP surface. The
// worker has no query API, so this is just liveness, readiness, and /metrics.
func AdminHandler(pool *pgxpool.Pool, rc rueidis.Client) http.Handler {
	return httpx.Traced("delivery-worker", httpx.AdminRouter(
		httpx.Dependency{Name: "postgres", Ping: pool.Ping},
		httpx.Dependency{Name: "redis", Ping: func(ctx context.Context) error {
			return rc.Do(ctx, rc.B().Ping().Build()).Error()
		}},
	))
}
