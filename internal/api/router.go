package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"github.com/redis/rueidis"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server bundles every dependency the API handlers need. Constructed once
// in main and passed by pointer.
type Server struct {
	cfg     Config
	lg      *zap.Logger
	pool    *pgxpool.Pool
	redis   rueidis.Client
	queries *gen.Queries
	cipher  *crypto.Cipher
	limiter *rateLimitStore
}

// NewServer wires every dependency. Caller owns pool/redis lifecycle.
func NewServer(cfg Config, lg *zap.Logger, pool *pgxpool.Pool, rc rueidis.Client, cipher *crypto.Cipher) *Server {
	return &Server{
		cfg:     cfg,
		lg:      lg,
		pool:    pool,
		redis:   rc,
		queries: gen.New(pool),
		cipher:  cipher,
		limiter: newRateLimitStore(),
	}
}

// Handler builds the full chi router with all middleware and routes wired.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(Obs())

	r.Get("/healthz", healthHandler())
	r.Get("/readyz", readyHandler(s.pool, s.redis))
	r.Handle("/metrics", metrics.Handler())

	if s.cfg.APIEnableSwagger {
		r.Get("/swagger/*", httpSwagger.Handler(
			httpSwagger.URL("/swagger/doc.json"),
		))
	}

	// Client-facing v1.
	r.Route("/v1", func(r chi.Router) {
		r.Use(ClientAuth(s.queries, s.lg))
		r.Use(RateLimit(s.limiter, s.lg))

		r.Post("/schedules", s.handleCreateSchedule)
		r.Get("/schedules/{schedule_id}", s.handleGetSchedule)
		r.Delete("/schedules/{schedule_id}", s.handleCancelSchedule)
	})

	// Admin.
	r.Route("/admin", func(r chi.Router) {
		r.Use(AdminAuth(s.cfg.AdminAPIKey))

		r.Post("/clients", s.handleCreateClient)
		r.Delete("/clients/{client_id}", s.handleDeleteClient)
		r.Post("/clients/{client_id}/providers", s.handleUpsertProvider)
		r.Delete("/clients/{client_id}/providers/{vendor}", s.handleDeleteProvider)
	})

	// Wrap with otelhttp so every request gets a span. Span name is the chi
	// route pattern injected via a custom formatter so spans group by route.
	return otelhttp.NewHandler(r, "scheduler-api",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			rc := chi.RouteContext(r.Context())
			if rc != nil && rc.RoutePattern() != "" {
				return r.Method + " " + rc.RoutePattern()
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}
