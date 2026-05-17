package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"Bearer ":        "",
		"Bearer xyz":     "xyz",
		"Bearer  spaced": "spaced",
		"Token abc":      "",
	}
	for header, want := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		got := bearerToken(req)
		if got != want {
			t.Errorf("bearerToken(%q) = %q want %q", header, got, want)
		}
	}
}

func TestSha256BytesDeterministic(t *testing.T) {
	a := sha256Bytes("hello")
	b := sha256Bytes("hello")
	if string(a) != string(b) {
		t.Fatal("sha256Bytes should be deterministic")
	}
	c := sha256Bytes("hello!")
	if string(a) == string(c) {
		t.Fatal("different inputs should hash differently")
	}
}

func TestAdminAuth(t *testing.T) {
	const key = "secret-admin-key"
	called := false
	h := AdminAuth(key)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("missing", func(t *testing.T) {
		called = false
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rr.Code)
		}
		if called {
			t.Fatal("downstream handler should not have been called")
		}
	})

	t.Run("wrong", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rr.Code)
		}
		var body apiError
		_ = json.NewDecoder(rr.Body).Decode(&body)
		if body.Error != ErrCodeUnauthorized {
			t.Fatalf("expected error=%q, got %q", ErrCodeUnauthorized, body.Error)
		}
	})

	t.Run("right", func(t *testing.T) {
		called = false
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d, body=%s", rr.Code, rr.Body.String())
		}
		if !called {
			t.Fatal("downstream handler should have been called")
		}
	})

	t.Run("admin response carries json envelope", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h.ServeHTTP(rr, req)
		if !strings.Contains(rr.Body.String(), `"error":"unauthorized"`) {
			t.Fatalf("unexpected body: %s", rr.Body.String())
		}
	})
}
