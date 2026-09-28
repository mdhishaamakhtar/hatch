// Package httpx holds the HTTP pieces shared by every Hatch service: JSON
// responses, bearer-token auth, and the health and metrics endpoints.
package httpx

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ErrorBody is the body of every non-2xx response. Clients branch on Error and,
// when present, Reason.
type ErrorBody struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
}

// WriteJSON writes v as the response body with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes an ErrorBody with the given status.
func WriteError(w http.ResponseWriter, status int, code, reason string) {
	WriteJSON(w, status, ErrorBody{Error: code, Reason: reason})
}

// BearerToken returns the token of an "Authorization: Bearer <token>" header,
// or "" if the request has none.
func BearerToken(r *http.Request) string {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(token)
}

// AdminAuth only lets through requests whose bearer token is key. An empty key
// admits nobody, so a missing secret can't turn authentication off.
func AdminAuth(key string) func(http.Handler) http.Handler {
	// Comparing digests keeps the comparison constant-time in the key's length
	// as well as its contents.
	want := sha256.Sum256([]byte(key))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := sha256.Sum256([]byte(BearerToken(r)))
			if key == "" || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				WriteError(w, http.StatusUnauthorized, "unauthorized", "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// A Check is a dependency that /readyz pings. Name is reported when it fails.
type Check struct {
	Name string
	Ping func(context.Context) error
}

// NewRouter returns a router serving what every Hatch service exposes:
// /healthz, /readyz and /metrics. Callers add their own routes to it.
//
// /healthz ignores dependencies on purpose: a pod whose database blips should
// be taken out of rotation by /readyz, not restarted by its liveness probe.
func NewRouter(checks ...Check) chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Tight, so a slow dependency reads as unready rather than as a hung probe.
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		for _, c := range checks {
			if err := c.Ping(ctx); err != nil {
				WriteError(w, http.StatusServiceUnavailable, "not_ready", c.Name)
				return
			}
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	r.Handle("/metrics", promhttp.Handler())
	return r
}
