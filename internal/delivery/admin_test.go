package delivery

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The admin surface is assembled at startup, and chi panics on a bad
// middleware/route ordering — a crash no compiler catches. Build it and serve a
// probe. The pool and Redis client are only touched inside /readyz, so nil deps
// are enough for the wiring check.
func TestAdminHandlerServesLiveness(t *testing.T) {
	rr := httptest.NewRecorder()
	AdminHandler(nil, nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rr.Code)
	}
}

func TestAdminHandlerServesMetrics(t *testing.T) {
	rr := httptest.NewRecorder()
	AdminHandler(nil, nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rr.Code)
	}
}
