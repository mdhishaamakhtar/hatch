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

// testRouter builds a router with cipher=nil (creds pass straight through),
// minReqs=2 and ratio=0.5 so two failures trip the breaker, and a long open
// timeout so it stays open for the duration of a test.
func testRouter(capacity, refill int, factories map[string]provider.Factory) *Router {
	return NewRouter(factories, nil, capacity, refill, 2, 0.5, time.Minute)
}

func factories(vendors ...string) map[string]provider.Factory {
	out := make(map[string]provider.Factory, len(vendors))
	for _, v := range vendors {
		out[v] = stubFactory(&stubProvider{vendor: v})
	}
	return out
}

func provs(vendors ...string) []cachedProvider {
	out := make([]cachedProvider, 0, len(vendors))
	for _, v := range vendors {
		out = append(out, cachedProvider{Vendor: v})
	}
	return out
}

// tripBreaker drives enough failures through a vendor to open its breaker.
func tripBreaker(r *Router, clientID, vendor string) {
	for range 2 {
		_ = r.Send(context.Background(), clientID, vendor, nil, provider.Email{})
	}
}

func TestSelectPrefersAnAlternativeToTheLastProvider(t *testing.T) {
	r := testRouter(100, 100, factories("mock", "resend"))

	sel := r.Select("c1", provs("mock", "resend"), "mock")

	if sel.Outcome != SelectOK || sel.Vendor != "resend" {
		t.Fatalf("Select = %+v, want resend (mock just failed)", sel)
	}
}

// A single-provider client must not be stranded after one transient blip: the
// last_provider exclusion is dropped when it would leave no candidate at all.
func TestSelectRetriesSoleProviderWhenExclusionEmptiesTheSet(t *testing.T) {
	r := testRouter(100, 100, factories("mock"))

	sel := r.Select("c1", provs("mock"), "mock")

	if sel.Outcome != SelectOK || sel.Vendor != "mock" {
		t.Fatalf("Select = %+v, want the sole provider retried", sel)
	}
}

// The relaxation above does NOT override an open breaker: a genuinely unhealthy
// sole provider yields no candidate, and the reason must say why.
func TestSelectReportsBreakerOpenForAnUnhealthySoleProvider(t *testing.T) {
	stub := &stubProvider{vendor: "mock", err: provider.ErrTransient}
	r := testRouter(100, 100, map[string]provider.Factory{"mock": stubFactory(stub)})
	tripBreaker(r, "c1", "mock")

	sel := r.Select("c1", provs("mock"), "mock")

	if sel.Outcome != SelectBreakerOpen {
		t.Fatalf("Outcome = %v, want SelectBreakerOpen", sel.Outcome)
	}
}

// An unregistered vendor is a configuration problem, and must be distinguishable
// from a transient one — it's the only outcome that may fail an email outright.
func TestSelectReportsNoEligibleVendorForUnregisteredVendors(t *testing.T) {
	r := testRouter(100, 100, factories("mock"))

	if sel := r.Select("c1", provs("sendgrid"), ""); sel.Outcome != SelectNoEligibleVendor {
		t.Errorf("unregistered vendor: Outcome = %v, want SelectNoEligibleVendor", sel.Outcome)
	}
	if sel := r.Select("c1", nil, ""); sel.Outcome != SelectNoEligibleVendor {
		t.Errorf("no providers at all: Outcome = %v, want SelectNoEligibleVendor", sel.Outcome)
	}
}

// An exhausted bucket is backpressure, not a failure — it has to be told apart
// from both of the above so the send is deferred rather than failed.
func TestSelectReportsNoCapacityWhenTheBucketIsEmpty(t *testing.T) {
	r := testRouter(1, 0, factories("mock"))

	if sel := r.Select("c1", provs("mock"), ""); sel.Outcome != SelectOK {
		t.Fatalf("first select should consume the single token, got %v", sel.Outcome)
	}
	if sel := r.Select("c1", provs("mock"), ""); sel.Outcome != SelectNoCapacity {
		t.Fatalf("Outcome = %v, want SelectNoCapacity", sel.Outcome)
	}
}

func TestSelectPicksTheVendorWithTheMostTokens(t *testing.T) {
	r := testRouter(100, 0, factories("mock", "resend"))
	r.mu.Lock()
	r.stateForLocked("c1", "mock").bucket.tokens = 1
	r.stateForLocked("c1", "resend").bucket.tokens = 50
	r.mu.Unlock()

	sel := r.Select("c1", provs("mock", "resend"), "")

	if sel.Outcome != SelectOK || sel.Vendor != "resend" {
		t.Fatalf("Select = %+v, want resend (more tokens)", sel)
	}
}

func TestSendTripsTheBreakerAfterRepeatedFailures(t *testing.T) {
	stub := &stubProvider{vendor: "mock", err: provider.ErrTransient}
	r := testRouter(100, 100, map[string]provider.Factory{"mock": stubFactory(stub)})

	tripBreaker(r, "c1", "mock")

	r.mu.Lock()
	state := r.stateForLocked("c1", "mock").breaker.State()
	r.mu.Unlock()
	if state != gobreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open after 2 failures", state)
	}
}

func TestRefillCapsAtCapacity(t *testing.T) {
	r := testRouter(10, 4, factories("mock"))
	r.mu.Lock()
	r.stateForLocked("c1", "mock").bucket.tokens = 9
	r.mu.Unlock()

	r.Refill() // 9 + 4 = 13, capped at 10

	r.mu.Lock()
	got := r.stateForLocked("c1", "mock").bucket.tokens
	r.mu.Unlock()
	if got != 10 {
		t.Fatalf("tokens = %d, want capacity 10", got)
	}
}

func TestVendorOfExtractsTheVendorFromAStateKey(t *testing.T) {
	if got := vendorOf(stateKey("client-1", "resend")); got != "resend" {
		t.Errorf("vendorOf = %q, want resend", got)
	}
	if got := vendorOf("no-separator"); got != "no-separator" {
		t.Errorf("vendorOf on a malformed key = %q, want the key itself", got)
	}
}
