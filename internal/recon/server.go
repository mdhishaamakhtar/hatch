package recon

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server is the reconciliation cron's health/observability HTTP surface. The
// service has no query API, so this is just liveness, readiness, and /metrics.
type Server struct {
	lg     *zap.Logger
	pool   *pgxpool.Pool
	broker *kgo.Client
}

// NewServer wires the admin surface. pool and broker are reused for the
// readiness ping; main owns their lifecycle.
func NewServer(lg *zap.Logger, pool *pgxpool.Pool, broker *kgo.Client) *Server {
	return &Server{lg: lg, pool: pool, broker: broker}
}

// Handler returns the chi router mounted by cmd/reconciliation-cron on the admin port.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)
	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())
	return otelhttp.NewHandler(r, "reconciliation-cron")
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
	if err := s.broker.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"kafka"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}
