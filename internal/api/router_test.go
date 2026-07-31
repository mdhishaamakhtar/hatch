package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

// Handler() wires middleware and routes onto one chi mux, and chi panics if any
// Use lands after a route is registered. That's a startup crash, not a compile
// error, so it needs a test that actually builds the router.
//
// The backing services are only touched inside request handlers, so a Server
// with nil deps is enough to exercise the wiring.
func testServer() *Server {
	return NewServer(Config{AdminAPIKey: "admin", APIEnableSwagger: true}, zap.NewNop(), nil, nil, nil)
}

func TestHandlerWiresWithoutPanicking(t *testing.T) {
	if h := testServer().Handler(); h == nil {
		t.Fatal("Handler returned nil")
	}
}

func TestHealthzIsServedAndUnauthenticated(t *testing.T) {
	rr := httptest.NewRecorder()
	testServer().Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rr.Code)
	}
}

func TestMetricsIsServed(t *testing.T) {
	rr := httptest.NewRecorder()
	testServer().Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rr.Code)
	}
}

// The v1 and admin groups must be behind their auth middleware — an unauthenticated
// request may never reach a handler that would touch the (here nil) database.
func TestProtectedRoutesRejectUnauthenticatedRequests(t *testing.T) {
	h := testServer().Handler()
	for _, path := range []string{"/v1/schedules", "/admin/clients"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, path, nil))
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("POST %s = %d, want 401", path, rr.Code)
		}
	}
}
