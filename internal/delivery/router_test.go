package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/sony/gobreaker/v2"
)

type stubProvider struct {
	vendor string
	err    error
	calls  int
}

func (s *stubProvider) Vendor() string                             { return s.vendor }
func (s *stubProvider) Send(context.Context, provider.Email) error { s.calls++; return s.err }

func stubFactory(p *stubProvider) provider.Factory {
	return func([]byte) (provider.Provider, error) { return p, nil }
}

func testRouter(t *testing.T, capacity, refill int, factories map[string]provider.Factory) *Router {
	t.Helper()
	// cipher=nil → creds pass straight through to the factory; minReqs=2 / ratio=0.5
	// so two failures trip the breaker; long open timeout so it stays open in-test.
	return NewRouter(factories, nil, capacity, refill, 2, 0.5, time.Minute)
}

func provs(vendors ...string) []cachedProvider {
	out := make([]cachedProvider, 0, len(vendors))
	for _, v := range vendors {
		out = append(out, cachedProvider{Vendor: v})
	}
	return out
}

func TestSelectExcludesLastProvider(t *testing.T) {
	r := testRouter(t, 100, 100, map[string]provider.Factory{
		"mock":   stubFactory(&stubProvider{vendor: "mock"}),
		"resend": stubFactory(&stubProvider{vendor: "resend"}),
	})
	vendor, _, ok := r.Select("c1", provs("mock", "resend"), "mock")
	if !ok || vendor != "resend" {
		t.Fatalf("want resend selected (mock excluded as last_provider), got %q ok=%v", vendor, ok)
	}
}

func TestSelectSkipsUnregisteredVendor(t *testing.T) {
	r := testRouter(t, 100, 100, map[string]provider.Factory{
		"mock": stubFactory(&stubProvider{vendor: "mock"}),
	})
	// sendgrid has no factory → no candidate.
	if _, _, ok := r.Select("c1", provs("sendgrid"), ""); ok {
		t.Fatal("want ok=false when no registered vendor matches")
	}
}

func TestSelectNoProviders(t *testing.T) {
	r := testRouter(t, 100, 100, map[string]provider.Factory{
		"mock": stubFactory(&stubProvider{vendor: "mock"}),
	})
	if _, _, ok := r.Select("c1", nil, ""); ok {
		t.Fatal("want ok=false with no providers")
	}
}

func TestSelectHighestTokenCount(t *testing.T) {
	r := testRouter(t, 100, 0, map[string]provider.Factory{
		"mock":   stubFactory(&stubProvider{vendor: "mock"}),
		"resend": stubFactory(&stubProvider{vendor: "resend"}),
	})
	// Drain mock's bucket below resend's so resend wins on capacity.
	r.mu.Lock()
	r.stateForLocked("c1", "mock").bucket.tokens = 1
	r.stateForLocked("c1", "resend").bucket.tokens = 50
	r.mu.Unlock()

	vendor, _, ok := r.Select("c1", provs("mock", "resend"), "")
	if !ok || vendor != "resend" {
		t.Fatalf("want resend (more tokens), got %q ok=%v", vendor, ok)
	}
}

func TestSelectConsumesTokenAndExhausts(t *testing.T) {
	r := testRouter(t, 1, 0, map[string]provider.Factory{
		"mock": stubFactory(&stubProvider{vendor: "mock"}),
	})
	if _, _, ok := r.Select("c1", provs("mock"), ""); !ok {
		t.Fatal("first select should succeed (1 token)")
	}
	if _, _, ok := r.Select("c1", provs("mock"), ""); ok {
		t.Fatal("second select should fail (bucket exhausted)")
	}
}

func TestSendTripsBreakerAndSelectExcludes(t *testing.T) {
	stub := &stubProvider{vendor: "mock", err: provider.ErrTransient}
	r := testRouter(t, 100, 100, map[string]provider.Factory{
		"mock": stubFactory(stub),
	})
	// Two failures with ratio 1.0 ≥ 0.5 over min 2 requests trips the breaker.
	for i := 0; i < 2; i++ {
		_ = r.Send(context.Background(), "c1", "mock", nil, provider.Email{})
	}
	r.mu.Lock()
	st := r.stateForLocked("c1", "mock")
	r.mu.Unlock()
	if st.breaker.State() != gobreaker.StateOpen {
		t.Fatalf("breaker should be OPEN after 2 failures, got %v", st.breaker.State())
	}
	if _, _, ok := r.Select("c1", provs("mock"), ""); ok {
		t.Fatal("Select should exclude a vendor whose breaker is OPEN")
	}
}

func TestRefillCapsAtCapacity(t *testing.T) {
	r := testRouter(t, 10, 4, map[string]provider.Factory{
		"mock": stubFactory(&stubProvider{vendor: "mock"}),
	})
	r.mu.Lock()
	st := r.stateForLocked("c1", "mock")
	st.bucket.tokens = 9
	r.mu.Unlock()
	r.Refill() // 9 + 4 = 13, capped to 10
	r.mu.Lock()
	got := r.stateForLocked("c1", "mock").bucket.tokens
	r.mu.Unlock()
	if got != 10 {
		t.Fatalf("tokens should cap at capacity 10, got %d", got)
	}
}
