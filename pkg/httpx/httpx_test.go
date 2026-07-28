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
		"Token abc":      "", // wrong scheme
		"bearer xyz":     "", // scheme is case-sensitive
	}
	for header, want := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		if got := BearerToken(req); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

func TestAdminAuth(t *testing.T) {
	const key = "secret-admin-key"
	var reached bool
	h := AdminAuth(key)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name       string
		header     string
		wantStatus int
		wantReach  bool
	}{
		{"missing header", "", http.StatusUnauthorized, false},
		{"wrong key", "Bearer wrong-key", http.StatusUnauthorized, false},
		{"correct key", "Bearer " + key, http.StatusOK, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reached = false
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			h.ServeHTTP(rr, req)

			if rr.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, c.wantStatus)
			}
			if reached != c.wantReach {
				t.Errorf("downstream reached = %v, want %v", reached, c.wantReach)
			}
			if !c.wantReach {
				var body ErrorBody
				_ = json.NewDecoder(rr.Body).Decode(&body)
				if body.Error != "unauthorized" {
					t.Errorf("error body = %+v, want error=unauthorized", body)
				}
			}
		})
	}
}

func TestWriteErrorAndWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	WriteError(rr, http.StatusTeapot, "teapot", "short_and_stout")

	if rr.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body ErrorBody
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Error != "teapot" || body.Reason != "short_and_stout" {
		t.Fatalf("body = %+v", body)
	}

	rr = httptest.NewRecorder()
	WriteJSON(rr, http.StatusCreated, map[string]string{"hello": "world"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rr.Code)
	}
	var m map[string]string
	_ = json.NewDecoder(rr.Body).Decode(&m)
	if m["hello"] != "world" {
		t.Fatalf("body = %+v", m)
	}
}

func TestAdminRouterHealthzIsAlwaysOK(t *testing.T) {
	// Liveness must not depend on backing services — a pod whose database is
	// down should be reported unready, not killed and restarted.
	r := AdminRouter(Dependency{Name: "postgres", Ping: func(context.Context) error {
		return errors.New("down")
	}})

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 even with a failing dependency", rr.Code)
	}
}

func TestAdminRouterReadyzReportsTheFailingDependency(t *testing.T) {
	up := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("connection refused") }

	cases := []struct {
		name       string
		deps       []Dependency
		wantStatus int
		wantReason string
	}{
		{"all up", []Dependency{{Name: "postgres", Ping: up}, {Name: "kafka", Ping: up}}, http.StatusOK, ""},
		{"first down", []Dependency{{Name: "postgres", Ping: down}, {Name: "kafka", Ping: up}}, http.StatusServiceUnavailable, "postgres"},
		{"second down", []Dependency{{Name: "postgres", Ping: up}, {Name: "kafka", Ping: down}}, http.StatusServiceUnavailable, "kafka"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			AdminRouter(c.deps...).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rr.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, c.wantStatus)
			}
			if c.wantReason != "" {
				var body ErrorBody
				_ = json.NewDecoder(rr.Body).Decode(&body)
				if body.Reason != c.wantReason {
					t.Errorf("reason = %q, want %q", body.Reason, c.wantReason)
				}
			}
		})
	}
}

func TestAdminRouterServesMetrics(t *testing.T) {
	rr := httptest.NewRecorder()
	AdminRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", rr.Code)
	}
}
