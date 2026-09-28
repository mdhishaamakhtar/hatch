package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/internal/provider"
	"github.com/sony/gobreaker/v2"
	"golang.org/x/time/rate"
)

// testRouter sends every route through p. Two failures in a row trip a
// breaker, which then stays open for the rest of the test.
func testRouter(p provider.Provider, limit rate.Limit) *router {
	r := newRouter(nil, provider.MockConfig{})
	r.newProvider = func(uuid.UUID, string, []byte) (provider.Provider, error) { return p, nil }
	r.limit = limit
	r.breaker.ReadyToTrip = func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 2 }
	r.breaker.Timeout = time.Minute
	return r
}

func vendors(names ...string) []cachedProvider {
	out := make([]cachedProvider, len(names))
	for i, n := range names {
		out[i] = cachedProvider{Vendor: n}
	}
	return out
}

var testClient = uuid.New()

func pickVendor(t *testing.T, r *router, providers []cachedProvider, lastFailed string) (string, error) {
	t.Helper()
	p, _, err := r.pick(testClient, providers, lastFailed)
	return p.Vendor, err
}

func tripBreaker(r *router, vendor string) {
	r.mu.Lock()
	rt := r.route(testClient, vendor)
	r.mu.Unlock()
	for range 2 {
		_, _ = rt.breaker.Execute(func() (struct{}, error) { return struct{}{}, errors.New("provider down") })
	}
}

// blockingProvider holds each send until released, announcing it on entered.
type blockingProvider struct{ entered, release chan struct{} }

func (b *blockingProvider) Send(context.Context, provider.Email) error {
	b.entered <- struct{}{}
	<-b.release
	return nil
}

func TestPickAvoidsTheProviderThatJustFailed(t *testing.T) {
	r := testRouter(&stubProvider{}, 100)
	if v, err := pickVendor(t, r, vendors("mock", "resend"), "mock"); v != "resend" || err != nil {
		t.Fatalf("picked %q, %v; want resend", v, err)
	}
}

// A client with one provider must not be stranded because it failed once.
func TestPickFallsBackToTheOnlyProvider(t *testing.T) {
	r := testRouter(&stubProvider{}, 100)
	if v, err := pickVendor(t, r, vendors("mock"), "mock"); v != "mock" || err != nil {
		t.Fatalf("picked %q, %v; want the sole provider", v, err)
	}
}

func TestPickSkipsOpenBreakers(t *testing.T) {
	r := testRouter(&stubProvider{}, 100)
	tripBreaker(r, "mock")

	if v, err := pickVendor(t, r, vendors("mock", "resend"), ""); v != "resend" || err != nil {
		t.Errorf("picked %q, %v; want the healthy provider", v, err)
	}
	if _, err := pickVendor(t, r, vendors("mock"), ""); !errors.Is(err, errBreakerOpen) {
		t.Errorf("sole provider's breaker open: err = %v, want errBreakerOpen", err)
	}
}

func TestPickWithoutProviders(t *testing.T) {
	r := testRouter(&stubProvider{}, 100)
	if _, err := pickVendor(t, r, nil, ""); !errors.Is(err, errNoProvider) {
		t.Fatalf("err = %v, want errNoProvider", err)
	}
}

func TestPickReportsExhaustedRateLimits(t *testing.T) {
	r := testRouter(&stubProvider{}, 1)
	if _, err := pickVendor(t, r, vendors("mock"), ""); err != nil {
		t.Fatalf("first pick: %v", err)
	}
	if _, err := pickVendor(t, r, vendors("mock"), ""); !errors.Is(err, errNoCapacity) {
		t.Fatalf("second pick: err = %v, want errNoCapacity", err)
	}
}

func TestPickPrefersTheMostHeadroom(t *testing.T) {
	r := testRouter(&stubProvider{}, 10)
	for range 5 {
		_, _ = pickVendor(t, r, vendors("mock"), "") // spend half of mock's tokens
	}
	if v, _ := pickVendor(t, r, vendors("mock", "resend"), ""); v != "resend" {
		t.Fatalf("picked %q, want resend, which has more headroom", v)
	}
}

// While a breaker is half-open it lets one probe through. A send that finds
// the probe already taken was never attempted, so it must be deferred rather
// than reported as a provider failure.
func TestSendDefersWhileTheHalfOpenProbeIsTaken(t *testing.T) {
	probe := &blockingProvider{entered: make(chan struct{}), release: make(chan struct{})}
	r := testRouter(probe, 100)
	r.breaker.Timeout = 10 * time.Millisecond
	tripBreaker(r, "mock")
	time.Sleep(20 * time.Millisecond) // past Timeout: the breaker half-opens on its next use

	go func() { _, _ = r.send(context.Background(), testClient, vendors("mock"), "", provider.Email{}) }()
	<-probe.entered
	defer close(probe.release)

	if _, err := r.send(context.Background(), testClient, vendors("mock"), "", provider.Email{}); !errors.Is(err, errBreakerOpen) {
		t.Fatalf("err = %v, want errBreakerOpen", err)
	}
}

func TestProviderIsRebuiltWhenCredentialsChange(t *testing.T) {
	r := testRouter(&stubProvider{}, 100)
	builds := 0
	r.newProvider = func(uuid.UUID, string, []byte) (provider.Provider, error) {
		builds++
		return &stubProvider{}, nil
	}
	send := func(creds string) {
		_, _ = r.send(context.Background(), testClient, []cachedProvider{{Vendor: "resend", Credentials: []byte(creds)}}, "", provider.Email{})
	}

	send("key-1")
	send("key-1")
	send("key-2")
	if builds != 2 {
		t.Fatalf("built the provider %d times, want 2 (once per credential)", builds)
	}
}
