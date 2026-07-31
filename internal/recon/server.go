package recon

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"github.com/twmb/franz-go/pkg/kgo"
)

// AdminHandler is the reconciliation cron's health/observability HTTP surface.
// The service has no query API, so this is just liveness, readiness, and
// /metrics.
func AdminHandler(pool *pgxpool.Pool, broker *kgo.Client) http.Handler {
	return httpx.Traced("reconciliation-cron", httpx.AdminRouter(
		httpx.Dependency{Name: "postgres", Ping: pool.Ping},
		httpx.Dependency{Name: "kafka", Ping: broker.Ping},
	))
}
