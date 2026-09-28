package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// The handlers only touch Postgres and Redis once a request is authenticated,
// so a Server without them is enough to test routing and auth.
func testServer() *Server {
	return New(Config{AdminAPIKey: "admin"}, zap.NewNop(), nil, nil, nil)
}

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

func TestHealthIsServedWithoutAuth(t *testing.T) {
	rr := httptest.NewRecorder()
	testServer().Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rr.Code)
	}
}

// limited runs one request through the rate limiter as client c.
func limited(s *Server, c authClient) int {
	h := s.rateLimit(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(context.WithValue(req.Context(), clientKey{}, c))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code
}

func TestRateLimitAllowsABurstThenRejects(t *testing.T) {
	s := testServer()
	c := authClient{id: uuid.New(), maxRPS: 5}
	for i := range 10 { // the burst is twice max_rps
		if code := limited(s, c); code != http.StatusOK {
			t.Fatalf("request %d of the burst: %d", i, code)
		}
	}
	if code := limited(s, c); code != http.StatusTooManyRequests {
		t.Fatalf("after the burst: %d, want 429", code)
	}
}

func TestRateLimitIsPerClient(t *testing.T) {
	s := testServer()
	noisy := authClient{id: uuid.New(), maxRPS: 1}
	for range 3 {
		limited(s, noisy)
	}
	if code := limited(s, authClient{id: uuid.New(), maxRPS: 1}); code != http.StatusOK {
		t.Fatalf("a quiet client got %d because of a noisy one", code)
	}
}

// Limiters live as long as the pod, so a changed max_rps has to retune the
// existing one rather than wait for a restart.
func TestRateLimitFollowsAChangedMaxRPS(t *testing.T) {
	s := testServer()
	id := uuid.New()
	for range 3 {
		limited(s, authClient{id: id, maxRPS: 1})
	}
	if code := limited(s, authClient{id: id, maxRPS: 1}); code != http.StatusTooManyRequests {
		t.Fatalf("at max_rps 1: %d, want 429", code)
	}
	limited(s, authClient{id: id, maxRPS: 1000})
	v, _ := s.limiters.Load(id)
	if got := v.(*rate.Limiter).Burst(); got != 2000 {
		t.Errorf("burst after raising max_rps = %d, want 2000", got)
	}
}

func TestStatusLabelKeepsCardinalityBounded(t *testing.T) {
	cases := map[int]string{200: "2xx", 201: "2xx", 204: "2xx", 400: "400", 404: "404", 429: "429", 418: "4xx", 499: "4xx", 500: "5xx", 503: "5xx"}
	for code, want := range cases {
		if got := statusLabel(code); got != want {
			t.Errorf("statusLabel(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		name, contentType, body string
		want                    int
	}{
		{"json", "application/json", `{"name":"x"}`, http.StatusOK},
		{"json with charset", "application/json; charset=utf-8", `{"name":"x"}`, http.StatusOK},
		{"no content type", "", `{"name":"x"}`, http.StatusOK},
		{"wrong content type", "text/plain", `{"name":"x"}`, http.StatusUnsupportedMediaType},
		{"malformed", "application/json", `{"name":`, http.StatusBadRequest},
		{"too large", "application/json", `{"name":"` + strings.Repeat("x", 100) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(c.body))
			req.Header.Set("Content-Type", c.contentType)
			rr := httptest.NewRecorder()
			var v createClientRequest
			if ok := decode(rr, req, 64, &v); ok != (c.want == http.StatusOK) || (!ok && rr.Code != c.want) {
				t.Fatalf("decode ok=%v status=%d, want %d", ok, rr.Code, c.want)
			}
		})
	}
}
