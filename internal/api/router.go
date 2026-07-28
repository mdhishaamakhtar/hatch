package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"github.com/redis/rueidis"
	httpSwagger "github.com/swaggo/http-swagger/v2"
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
	// chi panics if Use runs after a route is registered, so every middleware
	// goes on before MountAdmin below.
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(Obs())

	// The health/readiness/metrics surface is identical across every Hatch
	// service; only the routes below it are the API's own.
	httpx.MountAdmin(r,
		httpx.Dependency{Name: "postgres", Ping: s.pool.Ping},
		httpx.Dependency{Name: "redis", Ping: func(ctx context.Context) error {
			return s.redis.Do(ctx, s.redis.B().Ping().Build()).Error()
		}},
	)

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
		r.Use(httpx.AdminAuth(s.cfg.AdminAPIKey))

		r.Post("/clients", s.handleCreateClient)
		r.Delete("/clients/{client_id}", s.handleDeleteClient)
		r.Post("/clients/{client_id}/providers", s.handleUpsertProvider)
		r.Delete("/clients/{client_id}/providers/{vendor}", s.handleDeleteProvider)
	})

	// Every request gets a span named by its chi route pattern, so spans group
	// by route rather than by raw path.
	return httpx.Traced("scheduler-api", r)
}
