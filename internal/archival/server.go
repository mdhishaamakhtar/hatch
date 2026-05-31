package archival

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server is the archival cron's health/observability HTTP surface. The service
// has no query API, so this is just liveness, readiness, and /metrics.
type Server struct {
	lg   *zap.Logger
	pool *pgxpool.Pool
}

// NewServer wires the admin surface. pool is reused for the readiness ping; main
// owns its lifecycle.
func NewServer(lg *zap.Logger, pool *pgxpool.Pool) *Server {
	return &Server{lg: lg, pool: pool}
}

// Handler returns the chi router mounted by cmd/partition-archival on the admin port.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)
	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())
	return otelhttp.NewHandler(r, "partition-archival")
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"postgres"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}
