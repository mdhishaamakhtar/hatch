package delivery

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server is the delivery worker's health/observability HTTP surface. The worker
// has no query API, so this is just liveness, readiness, and /metrics.
type Server struct {
	lg   *zap.Logger
	pool *pgxpool.Pool
	rc   rueidis.Client
}

// NewServer wires the admin surface. Long-lived deps are owned by main.
func NewServer(lg *zap.Logger, pool *pgxpool.Pool, rc rueidis.Client) *Server {
	return &Server{lg: lg, pool: pool, rc: rc}
}

// Handler returns the chi router mounted by cmd/delivery-worker on the admin port.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)
	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())
	return otelhttp.NewHandler(r, "delivery-worker")
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
	if err := s.rc.Do(ctx, s.rc.B().Ping().Build()).Error(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"redis"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}
