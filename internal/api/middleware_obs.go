package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Obs wraps handlers to capture per-request metrics. The endpoint label
// uses the chi route pattern (not the raw path) to keep cardinality bounded.
func Obs() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			endpoint := r.Method + " " + chi.RouteContext(r.Context()).RoutePattern()
			if endpoint == r.Method+" " {
				endpoint = r.Method + " <unmatched>"
			}
			observeRequest(endpoint, ww.Status(), time.Since(start).Seconds())
		})
	}
}
