package scheduler

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// Server bundles every dependency the admin/observability HTTP surface needs.
// The 3 goroutines run independently and only share the wheel + pool/producer
// they need — Server is just the HTTP face.
type Server struct {
	cfg         Config
	lg          *zap.Logger
	pool        *pgxpool.Pool
	wheel       *Wheel
	storeOK     func() bool // bbolt readiness probe (always true once Open returns).
	producer    MessageProducer
	pollTrigger chan<- struct{} // signals the poller goroutine to run an out-of-band poll.
}

// NewServer wires the admin/observability HTTP surface. Long-lived dependencies
// (pool, producer, wheel, store) are passed in by main. pollTrigger is the
// out-of-band poll signal channel shared with RunPoller; a nil channel makes
// POST /internal/poll a no-op (used by tests).
func NewServer(
	cfg Config,
	lg *zap.Logger,
	pool *pgxpool.Pool,
	wheel *Wheel,
	storeOK func() bool,
	producer MessageProducer,
	pollTrigger chan<- struct{},
) *Server {
	return &Server{
		cfg:         cfg,
		lg:          lg,
		pool:        pool,
		wheel:       wheel,
		storeOK:     storeOK,
		producer:    producer,
		pollTrigger: pollTrigger,
	}
}

// Handler returns the full chi router. Mounted by cmd/scheduler on cfg.AdminPort.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())

	r.Route("/internal", func(r chi.Router) {
		r.Use(adminAuth(s.cfg.AdminAPIKey))
		r.Post("/poll", s.handlePoll)
		r.Get("/wheel/stats", s.handleStats)
		r.Get("/wheel/slots", s.handleSlots)
		r.Get("/wheel/slots/{mm}/{ss}", s.handleSlot)
	})

	return otelhttp.NewHandler(r, "scheduler-service",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			rc := chi.RouteContext(r.Context())
			if rc != nil && rc.RoutePattern() != "" {
				return r.Method + " " + rc.RoutePattern()
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "not_ready", "postgres")
		return
	}
	if !s.storeOK() {
		writeErr(w, http.StatusServiceUnavailable, "not_ready", "bbolt")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// handlePoll triggers an immediate, out-of-band poll cycle on this pod — the
// same code path as the hourly tick. Used by tooling (e.g. verification) to
// make the wheel pick up freshly-created rows without waiting for the next
// interval or restarting the pod. The send is non-blocking: if a poll is
// already queued the signal coalesces. Returns 202 regardless.
func (s *Server) handlePoll(w http.ResponseWriter, _ *http.Request) {
	select {
	case s.pollTrigger <- struct{}{}:
		s.lg.Info("manual poll triggered", zap.Int("pod_index", s.cfg.PodIndex))
	default:
		// A poll is already pending (or no trigger is wired) — coalesce.
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "poll_triggered"})
}

type wheelStats struct {
	PodIndex      int `json:"pod_index"`
	TotalPods     int `json:"total_pods"`
	OccupiedSlots int `json:"occupied_slots"`
	TotalLoaded   int `json:"total_loaded"`
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	occ, total := s.wheel.Stats()
	writeJSON(w, http.StatusOK, wheelStats{
		PodIndex:      s.cfg.PodIndex,
		TotalPods:     s.cfg.TotalPods,
		OccupiedSlots: occ,
		TotalLoaded:   total,
	})
}

func (s *Server) handleSlots(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"pod_index": s.cfg.PodIndex,
		"slots":     s.wheel.Slots(),
	})
}

func (s *Server) handleSlot(w http.ResponseWriter, r *http.Request) {
	mm, ssOK := parsePathInt(chi.URLParam(r, "mm"))
	ss, mmOK := parsePathInt(chi.URLParam(r, "ss"))
	if !ssOK || !mmOK || mm >= SlotsPerDim || ss >= SlotsPerDim {
		writeErr(w, http.StatusBadRequest, "bad_slot", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"slot":         SlotKey(mm, ss),
		"schedule_ids": s.wheel.Slot(mm, ss),
	})
}

func parsePathInt(s string) (int, bool) {
	if len(s) == 0 || len(s) > 2 {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// adminAuth gates /internal/* behind the static admin Bearer token — same
// shape as the API service's AdminAuth, repeated here so the scheduler doesn't
// depend on internal/api.
func adminAuth(adminKey string) func(http.Handler) http.Handler {
	expected := []byte(adminKey)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			v := r.Header.Get("Authorization")
			tok := ""
			if strings.HasPrefix(v, "Bearer ") {
				tok = strings.TrimSpace(v[len("Bearer "):])
			}
			if subtle.ConstantTimeCompare([]byte(tok), expected) != 1 {
				writeErr(w, http.StatusUnauthorized, "unauthorized", "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type errBody struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
}

func writeErr(w http.ResponseWriter, status int, code, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errBody{Error: code, Reason: reason})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
