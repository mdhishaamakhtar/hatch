// Package httpx holds the HTTP pieces every Hatch service shares: JSON
// response helpers, Bearer-token extraction and admin auth, and the
// liveness/readiness/metrics surface the four non-API services expose verbatim.
package httpx

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ErrorBody is the response shape for every non-2xx across all services, so
// clients can branch on `error` and (when present) `reason`.
type ErrorBody struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
}

// WriteJSON encodes v as the response body with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError is the single path for non-2xx responses.
func WriteError(w http.ResponseWriter, status int, code, reason string) {
	WriteJSON(w, status, ErrorBody{Error: code, Reason: reason})
}

// BearerToken extracts the token from an "Authorization: Bearer <x>" header.
// Returns "" if the header is missing or malformed.
func BearerToken(r *http.Request) string {
	const prefix = "Bearer "
	v := r.Header.Get("Authorization")
	rest, ok := strings.CutPrefix(v, prefix)
	if !ok {
		return ""
	}
	return strings.TrimSpace(rest)
}

// AdminAuth gates a route group behind a single static shared key, compared in
// constant time.
func AdminAuth(adminKey string) func(http.Handler) http.Handler {
	expected := []byte(adminKey)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if subtle.ConstantTimeCompare([]byte(BearerToken(r)), expected) != 1 {
				WriteError(w, http.StatusUnauthorized, "unauthorized", "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// readyTimeout bounds every dependency ping on /readyz. Kept tight so a slow
// dependency shows up as unready rather than as a hanging probe.
const readyTimeout = 500 * time.Millisecond

// Dependency is one backing service a readiness probe checks. Name is the
// `reason` reported when Ping fails (e.g. "postgres", "kafka").
type Dependency struct {
	Name string
	Ping func(context.Context) error
}

// AdminRouter builds the liveness/readiness/metrics surface shared by the
// delivery worker, retry consumer, reconciliation cron, and partition archival
// cron — services with no query API. The returned router can be extended with
// further *routes* (the scheduler adds its /internal/* group).
//
// A caller that needs its own middleware must build the router itself and call
// MountAdmin last: chi panics if Use runs after a route is registered.
func AdminRouter(deps ...Dependency) chi.Router {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RequestID)
	MountAdmin(r, deps...)
	return r
}

// MountAdmin registers /healthz, /readyz, and /metrics on an existing router.
// Call it after every Use, before or alongside the caller's own routes.
//
// /healthz is unconditional — liveness must not depend on backing services, or
// a database blip gets the pod killed instead of merely taken out of rotation.
// /readyz pings deps in order and reports the first failure as 503; the process
// keeps running through the outage so /metrics stays scrapable.
func MountAdmin(r chi.Router, deps ...Dependency) {
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		for _, d := range deps {
			if err := d.Ping(ctx); err != nil {
				WriteError(w, http.StatusServiceUnavailable, "not_ready", d.Name)
				return
			}
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	r.Handle("/metrics", metrics.Handler())
}

// Traced wraps a handler so every request gets a span named by the chi route
// pattern (falling back to the raw path), keeping span names low-cardinality.
func Traced(service string, h http.Handler) http.Handler {
	return otelhttp.NewHandler(h, service,
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
				return r.Method + " " + rc.RoutePattern()
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}
