package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestRateLimitAllowsThenBlocks(t *testing.T) {
	store := newRateLimitStore()
	lg := zap.NewNop()
	clientID := uuid.New()

	const rps = 5
	// Burst is rps*2 = 10. The 11th call inside the same instant should 429.
	h := RateLimit(store, lg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < rps*2; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctxWithClient(clientID, rps))
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("burst request %d: expected 200, got %d", i, rr.Code)
		}
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctxWithClient(clientID, rps))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("post-burst: expected 429, got %d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After: want 1, got %q", got)
	}
}

func TestRateLimitNoClientPassesThrough(t *testing.T) {
	store := newRateLimitStore()
	lg := zap.NewNop()

	called := false
	h := RateLimit(store, lg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)
	if !called || rr.Code != http.StatusOK {
		t.Fatalf("unauthenticated path should pass through; called=%v code=%d", called, rr.Code)
	}
}

func ctxWithClient(id uuid.UUID, rps int32) context.Context {
	ctx := withClientID(context.Background(), id)
	ctx = withMaxRPS(ctx, rps)
	return ctx
}
