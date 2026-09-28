package scheduler

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
)

// Handler serves the health endpoints and this pod's admin API:
//
//	POST /internal/poll         load schedules created since their window was polled
//	GET  /internal/wheel/stats  what this pod's wheel holds
func (s *Scheduler) Handler(pool *pgxpool.Pool) http.Handler {
	r := httpx.NewRouter(
		httpx.Check{Name: "postgres", Ping: pool.Ping},
		httpx.Check{Name: "wheel", Ping: func(context.Context) error {
			_, err := s.wheel.size()
			return err
		}},
	)
	r.Route("/internal", func(r chi.Router) {
		r.Use(httpx.AdminAuth(s.cfg.AdminAPIKey))
		r.Post("/poll", func(w http.ResponseWriter, _ *http.Request) {
			select {
			case s.pollNow <- struct{}{}:
			default: // a poll is already pending
			}
			httpx.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "poll_triggered"})
		})
		r.Get("/wheel/stats", func(w http.ResponseWriter, _ *http.Request) {
			n, err := s.wheel.size()
			if err != nil {
				httpx.WriteError(w, http.StatusInternalServerError, "internal", "")
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]int{
				"pod_index":    s.cfg.PodIndex,
				"total_pods":   s.cfg.TotalPods,
				"total_loaded": n,
			})
		})
	})
	return r
}
