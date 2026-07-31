package recon

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The admin surface is assembled at startup, and chi panics on a bad
// middleware/route ordering — a crash no compiler catches. Build it and serve a
// probe; the backing clients are only touched inside /readyz.
func TestAdminHandlerServesLiveness(t *testing.T) {
	rr := httptest.NewRecorder()
	AdminHandler(nil, nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rr.Code)
	}
}
