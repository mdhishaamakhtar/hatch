package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"Bearer ":        "",
		"Bearer xyz":     "xyz",
		"Bearer  spaced": "spaced",
		"Token abc":      "",
		"bearer xyz":     "",
	}
	for header, want := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", header)
		if got := BearerToken(req); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

func TestAdminAuth(t *testing.T) {
	cases := []struct {
		name, key, header string
		want              int
	}{
		{"correct key", "secret", "Bearer secret", http.StatusOK},
		{"wrong key", "secret", "Bearer nope", http.StatusUnauthorized},
		{"missing header", "secret", "", http.StatusUnauthorized},
		// A deployment with the key left blank must not accept blank tokens.
		{"unset key, no header", "", "", http.StatusUnauthorized},
		{"unset key, empty token", "", "Bearer ", http.StatusUnauthorized},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := AdminAuth(c.key)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", c.header)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != c.want {
				t.Fatalf("status = %d, want %d", rr.Code, c.want)
			}
		})
	}
}

func TestHealthzIgnoresDependencies(t *testing.T) {
	r := NewRouter(Check{Name: "postgres", Ping: func(context.Context) error { return errors.New("down") }})
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200 despite a failing dependency", rr.Code)
	}
}

func TestReadyzNamesTheFailingDependency(t *testing.T) {
	up := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("connection refused") }

	cases := []struct {
		name       string
		checks     []Check
		wantStatus int
		wantReason string
	}{
		{"all up", []Check{{"postgres", up}, {"kafka", up}}, http.StatusOK, ""},
		{"first down", []Check{{"postgres", down}, {"kafka", up}}, http.StatusServiceUnavailable, "postgres"},
		{"second down", []Check{{"postgres", up}, {"kafka", down}}, http.StatusServiceUnavailable, "kafka"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			NewRouter(c.checks...).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rr.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, c.wantStatus)
			}
			var body ErrorBody
			_ = json.NewDecoder(rr.Body).Decode(&body)
			if body.Reason != c.wantReason {
				t.Errorf("reason = %q, want %q", body.Reason, c.wantReason)
			}
		})
	}
}

func TestMetricsIsServed(t *testing.T) {
	rr := httptest.NewRecorder()
	NewRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rr.Code)
	}
}
