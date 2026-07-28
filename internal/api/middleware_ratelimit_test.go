package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// callN issues n requests through the middleware and returns the status codes.
func callN(h http.Handler, ctx context.Context, n int) []int {
	codes := make([]int, 0, n)
	for range n {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
		codes = append(codes, rr.Code)
	}
	return codes
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func ctxWithClient(id uuid.UUID, rps int32) context.Context {
	return withMaxRPS(withClientID(context.Background(), id), rps)
}

func TestRateLimitAllowsTheBurstThenRejects(t *testing.T) {
	const rps = 5
	h := RateLimit(newRateLimitStore(), zap.NewNop())(okHandler())
	ctx := ctxWithClient(uuid.New(), rps)

	// Burst is rps*2; the next request inside the same instant must 429.
	for i, code := range callN(h, ctx, burstFor(rps)) {
		if code != http.StatusOK {
			t.Fatalf("burst request %d: got %d, want 200", i, code)
		}
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("post-burst: got %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

// The limiter is cached per client for the pod's lifetime, so it has to be
// retuned when an admin changes that client's max_rps — otherwise the new limit
// only takes effect after an API restart.
//
// Retuning does not retroactively grant tokens (x/time/rate refills from the
// clock), so this asserts the limiter's configuration, not an instant allow.
func TestRateLimitRetunesWhenMaxRPSChanges(t *testing.T) {
	store := newRateLimitStore()
	clientID := uuid.New()

	limiter := store.limiterFor(clientID, 1)
	limiter.allow(1)
	if got := limiter.limiter.Limit(); got != 1 {
		t.Fatalf("initial limit = %v, want 1", got)
	}

	// A later request carrying the raised max_rps retunes the cached limiter.
	limiter.allow(100)

	if got := limiter.limiter.Limit(); got != 100 {
		t.Errorf("limit after change = %v, want 100", got)
	}
	if got := limiter.limiter.Burst(); got != burstFor(100) {
		t.Errorf("burst after change = %d, want %d", got, burstFor(100))
	}

	// And the same limiter instance is still the one the middleware uses.
	if store.limiterFor(clientID, 100) != limiter {
		t.Error("retuning should update the cached limiter, not replace it")
	}
}

func TestRateLimitIsPerClient(t *testing.T) {
	store := newRateLimitStore()
	h := RateLimit(store, zap.NewNop())(okHandler())

	noisy := ctxWithClient(uuid.New(), 1)
	callN(h, noisy, burstFor(1)+1) // exhaust one client

	quiet := ctxWithClient(uuid.New(), 1)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(quiet))
	if rr.Code != http.StatusOK {
		t.Fatalf("one client's burst must not throttle another, got %d", rr.Code)
	}
}

// Requests without auth context are let through: an upstream 401 has already
// been written, and the limiter has no client to key on.
func TestRateLimitPassesThroughUnauthenticatedRequests(t *testing.T) {
	called := false
	h := RateLimit(newRateLimitStore(), zap.NewNop())(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if !called || rr.Code != http.StatusOK {
		t.Fatalf("unauthenticated path should pass through; called=%v code=%d", called, rr.Code)
	}
}
