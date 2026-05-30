package retry

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server is the retry consumer's health/observability HTTP surface. The service
// has no query API, so this is just liveness, readiness, and /metrics.
type Server struct {
	lg     *zap.Logger
	broker *kgo.Client
}

// NewServer wires the admin surface. broker is the producer client, reused for
// the readiness ping; main owns its lifecycle.
func NewServer(lg *zap.Logger, broker *kgo.Client) *Server {
	return &Server{lg: lg, broker: broker}
}

// Handler returns the chi router mounted by cmd/retry-consumer on the admin port.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)
	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())
	return otelhttp.NewHandler(r, "retry-consumer")
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if err := s.broker.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"kafka"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}
