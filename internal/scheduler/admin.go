package scheduler

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"go.uber.org/zap"
)

// errStoreUnavailable is the readiness failure reported when the bbolt handle
// isn't usable.
var errStoreUnavailable = errors.New("bbolt store unavailable")

// Server is the scheduler's admin/observability HTTP surface: the shared
// health/readiness/metrics routes plus /internal/* for inspecting and nudging
// the wheel.
type Server struct {
	cfg      Config
	lg       *zap.Logger
	pool     *pgxpool.Pool
	pipeline *Pipeline
	storeOK  func() bool // bbolt readiness probe (always true once Open returns)
}

// NewServer wires the admin surface. Long-lived dependencies are owned by main.
func NewServer(cfg Config, lg *zap.Logger, pool *pgxpool.Pool, pipeline *Pipeline, storeOK func() bool) *Server {
	return &Server{cfg: cfg, lg: lg, pool: pool, pipeline: pipeline, storeOK: storeOK}
}

// Handler returns the full chi router. Mounted by cmd/scheduler on cfg.AdminPort.
func (s *Server) Handler() http.Handler {
	r := httpx.AdminRouter(
		httpx.Dependency{Name: "postgres", Ping: s.pool.Ping},
		httpx.Dependency{Name: "bbolt", Ping: func(context.Context) error {
			if s.storeOK() {
				return nil
			}
			return errStoreUnavailable
		}},
	)

	r.Route("/internal", func(r chi.Router) {
		r.Use(httpx.AdminAuth(s.cfg.AdminAPIKey))
		r.Post("/poll", s.handlePoll)
		r.Get("/wheel/stats", s.handleStats)
		r.Get("/wheel/slots", s.handleSlots)
		r.Get("/wheel/slots/{mm}/{ss}", s.handleSlot)
	})

	return httpx.Traced("scheduler-service", r)
}

// handlePoll triggers an immediate, out-of-band poll cycle on this pod — the
// same code path as the hourly tick. Used by tooling (e.g. verification) to make
// the wheel pick up freshly-created rows without waiting for the next interval
// or restarting the pod. The send is non-blocking: if a poll is already queued
// the signal coalesces. Returns 202 regardless.
func (s *Server) handlePoll(w http.ResponseWriter, _ *http.Request) {
	select {
	case s.pipeline.PollTrigger() <- struct{}{}:
		s.lg.Info("manual poll triggered", zap.Int("pod_index", s.cfg.PodIndex))
	default:
		// A poll is already pending — coalesce.
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "poll_triggered"})
}

type wheelStats struct {
	PodIndex      int `json:"pod_index"`
	TotalPods     int `json:"total_pods"`
	OccupiedSlots int `json:"occupied_slots"`
	TotalLoaded   int `json:"total_loaded"`
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	occupied, total := s.pipeline.wheel.Stats()
	httpx.WriteJSON(w, http.StatusOK, wheelStats{
		PodIndex:      s.cfg.PodIndex,
		TotalPods:     s.cfg.TotalPods,
		OccupiedSlots: occupied,
		TotalLoaded:   total,
	})
}

func (s *Server) handleSlots(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"pod_index": s.cfg.PodIndex,
		"slots":     s.pipeline.wheel.Slots(),
	})
}

func (s *Server) handleSlot(w http.ResponseWriter, r *http.Request) {
	mm, errMin := strconv.Atoi(chi.URLParam(r, "mm"))
	ss, errSec := strconv.Atoi(chi.URLParam(r, "ss"))
	slot := Slot{Min: mm, Sec: ss}
	if errMin != nil || errSec != nil || !slot.valid() {
		httpx.WriteError(w, http.StatusBadRequest, "bad_slot", "")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"slot":         slot.String(),
		"schedule_ids": s.pipeline.wheel.Slot(slot),
	})
}
