package archival

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
)

// AdminHandler is the archival cron's health/observability HTTP surface. The
// service has no query API, so this is just liveness, readiness, and /metrics.
func AdminHandler(pool *pgxpool.Pool) http.Handler {
	return httpx.Traced("partition-archival", httpx.AdminRouter(
		httpx.Dependency{Name: "postgres", Ping: pool.Ping},
	))
}
