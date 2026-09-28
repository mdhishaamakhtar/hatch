// Package api is the scheduler API. Clients create, read and cancel scheduled
// emails under /v1; the admin creates clients and registers their providers
// under /admin.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/internal/crypto"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/rueidis"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Config is read from the environment.
type Config struct {
	DatabaseURL     string `env:"DATABASE_URL,required,notEmpty"`
	RedisAddr       string `env:"REDIS_ADDR,required,notEmpty"`
	AdminAPIKey     string `env:"ADMIN_API_KEY,required,notEmpty"`
	ProviderCredKey string `env:"PROVIDER_CRED_KEY,required,notEmpty"`
	Port            int    `env:"PORT" envDefault:"9021"`

	// MinScheduleHorizon is how far ahead a schedule has to be due. The
	// scheduler loads an hour ahead, so a schedule due any sooner is only seen
	// by an on-demand poll; lower this only where something triggers those.
	MinScheduleHorizon time.Duration `env:"API_MIN_SCHEDULE_HORIZON" envDefault:"1h"`
}

// The codes a response's "error" field can carry.
const (
	codeUnauthorized      = "unauthorized"
	codeRateLimited       = "rate_limited"
	codeValidation        = "validation_failed"
	codeNoActiveProviders = "no_active_providers"
	codeNotFound          = "not_found"
	codeConflict          = "conflict"
	codeTooLarge          = "payload_too_large"
	codeUnsupportedMedia  = "unsupported_media_type"
	codeInternal          = "internal"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_api_requests_total",
		Help: "API requests by route and status code.",
	}, []string{"endpoint", "status_code"})
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hatch_api_request_duration_seconds",
		Help:    "API request latency by route.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"endpoint"})
	validationFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_api_validation_failures_total",
		Help: "Rejected request bodies by reason.",
	}, []string{"reason"})
	idempotencyHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_api_idempotency_hits_total",
		Help: "Schedule creates answered from an idempotency key used before.",
	})
	// No client_id label on the next two: a series per client grows without
	// bound, and the logs already name the client.
	noProviderRejections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_api_no_provider_rejections_total",
		Help: "Schedule creates rejected because the client has no active provider.",
	})
	rateLimited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_api_rate_limited_total",
		Help: "Requests rejected by the per-client rate limit.",
	})
)

// Server handles the API's requests.
type Server struct {
	cfg      Config
	lg       *zap.Logger
	pool     *pgxpool.Pool
	queries  *db.Queries
	redis    rueidis.Client
	cipher   *crypto.Cipher
	limiters sync.Map // client id → *rate.Limiter
}

// New returns a Server. The caller owns pool and redis.
func New(cfg Config, lg *zap.Logger, pool *pgxpool.Pool, redis rueidis.Client, cipher *crypto.Cipher) *Server {
	return &Server{cfg: cfg, lg: lg, pool: pool, queries: db.New(pool), redis: redis, cipher: cipher}
}

// Handler routes the API.
func (s *Server) Handler() http.Handler {
	r := httpx.NewRouter(
		httpx.Check{Name: "postgres", Ping: s.pool.Ping},
		httpx.Check{Name: "redis", Ping: func(ctx context.Context) error {
			return s.redis.Do(ctx, s.redis.B().Ping().Build()).Error()
		}},
	)
	r.Get("/swagger/*", httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")))
	r.Group(func(r chi.Router) {
		r.Use(otelhttp.NewMiddleware("scheduler-api"), observe)
		r.Route("/v1", func(r chi.Router) {
			r.Use(s.authenticate, s.rateLimit)
			r.Post("/schedules", s.createSchedule)
			r.Get("/schedules/{schedule_id}", s.getSchedule)
			r.Delete("/schedules/{schedule_id}", s.cancelSchedule)
		})
		r.Route("/admin", func(r chi.Router) {
			r.Use(httpx.AdminAuth(s.cfg.AdminAPIKey))
			r.Post("/clients", s.createClient)
			r.Delete("/clients/{client_id}", s.deleteClient)
			r.Post("/clients/{client_id}/providers", s.upsertProvider)
			r.Delete("/clients/{client_id}/providers/{vendor}", s.deleteProvider)
		})
	})
	return r
}

// observe records each request's metrics, and names its trace span, by the
// route it matched rather than its raw path, which keeps both bounded.
func observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := r.Method + " " + chi.RouteContext(r.Context()).RoutePattern()
		trace.SpanFromContext(r.Context()).SetName(route)
		requests.WithLabelValues(route, statusLabel(ww.Status())).Inc()
		requestDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

// statusLabel keeps the status_code label bounded: the 4xx codes the API
// returns stay exact, anything else is reduced to its class.
func statusLabel(code int) string {
	switch code {
	case 400, 401, 404, 409, 413, 415, 429:
		return strconv.Itoa(code)
	}
	return strconv.Itoa(code/100) + "xx"
}

type clientKey struct{}

// authClient is the client a /v1 request authenticated as.
type authClient struct {
	id          uuid.UUID
	maxRPS      int32
	hasProvider bool
}

func clientFrom(ctx context.Context) authClient {
	c, _ := ctx.Value(clientKey{}).(authClient)
	return c
}

// authenticate finds the client whose API key the request carries. Keys are
// random 256-bit values, so finding the client by the key's SHA-256 is the
// credential check itself: only the key's holder can produce that digest.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := httpx.BearerToken(r)
		if key == "" {
			httpx.WriteError(w, http.StatusUnauthorized, codeUnauthorized, "missing_bearer")
			return
		}
		digest := sha256.Sum256([]byte(key))
		row, err := s.queries.GetClientByAPIKey(r.Context(), digest[:])
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, http.StatusUnauthorized, codeUnauthorized, "unknown_key")
			return
		}
		if err != nil {
			s.internalError(w, r, "authenticate client", err)
			return
		}
		c := authClient{id: uuid.UUID(row.ID), maxRPS: row.MaxRps, hasProvider: row.HasProvider}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKey{}, c)))
	})
}

// rateLimit holds each client to its max_rps, with a one-second burst at twice
// that rate. A limiter lives as long as the pod, and is retuned in place when
// the client's max_rps changes.
func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := clientFrom(r.Context())
		limit := rate.Limit(max(c.maxRPS, 1))
		v, ok := s.limiters.Load(c.id)
		if !ok {
			v, _ = s.limiters.LoadOrStore(c.id, rate.NewLimiter(limit, 2*int(limit)))
		}
		limiter := v.(*rate.Limiter)
		if limiter.Limit() != limit {
			limiter.SetLimit(limit)
			limiter.SetBurst(2 * int(limit))
		}
		if !limiter.Allow() {
			rateLimited.Inc()
			s.lg.Warn("rate limited", zap.Stringer("client_id", c.id))
			w.Header().Set("Retry-After", "1")
			httpx.WriteError(w, http.StatusTooManyRequests, codeRateLimited, "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// decode reads a JSON body of at most limit bytes into v. When it can't, it
// answers the request itself and returns false.
func decode(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if mediaType, _, _ := mime.ParseMediaType(ct); mediaType != "application/json" {
			httpx.WriteError(w, http.StatusUnsupportedMediaType, codeUnsupportedMedia, "")
			return false
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		httpx.WriteError(w, http.StatusRequestEntityTooLarge, codeTooLarge, "")
		return false
	case err != nil:
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "body_read")
		return false
	}
	if err := json.Unmarshal(body, v); err != nil {
		validationFailures.WithLabelValues("json").Inc()
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "json")
		return false
	}
	return true
}

// pathID parses the UUID in URL parameter name. When it can't, it answers the
// request itself and returns false.
func pathID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, name))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, name+"_invalid")
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, what string, err error) {
	service.WithTrace(r.Context(), s.lg).Error(what+" failed", zap.Error(err))
	httpx.WriteError(w, http.StatusInternalServerError, codeInternal, "")
}

// invalidateClient drops the delivery worker's cached copy of a client, so a
// change to it or its providers is seen on the next send. A failure only
// delays that until the cache entry expires.
func (s *Server) invalidateClient(ctx context.Context, id uuid.UUID) {
	if err := s.redis.Do(ctx, s.redis.B().Del().Key("client:"+id.String()).Build()).Error(); err != nil {
		s.lg.Warn("client cache invalidation failed", zap.Stringer("client_id", id), zap.Error(err))
	}
}
