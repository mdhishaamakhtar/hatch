package delivery

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/sony/gobreaker/v2"
)

// tokenBucket is a leaky bucket with an inspectable integer token count. G3
// refills it each tick; the selection algorithm reads the count and consumes a
// token. Not goroutine-safe on its own — the Router's RWMutex guards every access.
type tokenBucket struct {
	tokens   int
	capacity int
	refill   int
}

func (b *tokenBucket) topUp() {
	b.tokens += b.refill
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
}

func (b *tokenBucket) take() bool {
	if b.tokens > 0 {
		b.tokens--
		return true
	}
	return false
}

// vendorState is the per-(client, vendor) routing state: a lazily-built provider
// (rebuilt if the client's credentials change), a circuit breaker, and a leaky
// bucket. Breaker and bucket persist across credential rotations.
type vendorState struct {
	provider provider.Provider
	credsRaw []byte
	breaker  *gobreaker.CircuitBreaker[any]
	bucket   *tokenBucket
}

// Router selects a provider per send and shields each provider behind a circuit
// breaker + leaky bucket. State is keyed by (client_id, vendor).
type Router struct {
	mu        sync.RWMutex
	factories map[string]provider.Factory
	cipher    *crypto.Cipher
	states    map[string]*vendorState

	capacity int
	refill   int

	breakerMinReqs uint32
	breakerRatio   float64
	breakerTimeout time.Duration
}

// NewRouter builds a router with the given vendor factories and tuning. capacity
// is the leaky-bucket size, refill is the tokens added per G3 tick.
func NewRouter(
	factories map[string]provider.Factory,
	cipher *crypto.Cipher,
	capacity, refill int,
	breakerMinReqs uint32,
	breakerRatio float64,
	breakerOpenTimeout time.Duration,
) *Router {
	return &Router{
		factories:      factories,
		cipher:         cipher,
		states:         make(map[string]*vendorState),
		capacity:       capacity,
		refill:         refill,
		breakerMinReqs: breakerMinReqs,
		breakerRatio:   breakerRatio,
		breakerTimeout: breakerOpenTimeout,
	}
}

func stateKey(clientID, vendor string) string { return clientID + "|" + vendor }

// Refill tops up every bucket. Called by G3 on each tick.
func (r *Router) Refill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, st := range r.states {
		st.bucket.topUp()
		mBucketTokens.WithLabelValues(vendorOf(k)).Set(float64(st.bucket.tokens))
	}
}

// Select runs the LLD selection algorithm and consumes one token from the chosen
// vendor's bucket. Returns ok=false when no vendor remains (no registered impl,
// excluded by last_provider, OPEN breaker, or no capacity).
func (r *Router) Select(clientID string, providers []cachedProvider, lastProvider string) (vendor string, creds []byte, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	bestTokens := 0
	var best *vendorState
	for _, p := range providers {
		if _, has := r.factories[p.Vendor]; !has {
			continue // no implementation registered for this vendor
		}
		if p.Vendor == lastProvider {
			continue // don't immediately retry the vendor that just failed
		}
		st := r.stateForLocked(clientID, p.Vendor)
		if st.breaker.State() == gobreaker.StateOpen {
			continue
		}
		if st.bucket.tokens > bestTokens {
			bestTokens = st.bucket.tokens
			best = st
			vendor = p.Vendor
			creds = p.Credentials
		}
	}
	if best == nil || !best.bucket.take() {
		return "", nil, false
	}
	mBucketTokens.WithLabelValues(vendor).Set(float64(best.bucket.tokens))
	return vendor, creds, true
}

// Send builds (or reuses) the per-client provider and runs the send through the
// vendor's circuit breaker. The network call happens outside the lock.
func (r *Router) Send(ctx context.Context, clientID, vendor string, creds []byte, e provider.Email) error {
	p, breaker, err := r.providerFor(clientID, vendor, creds)
	if err != nil {
		return err // credential/build failure — treated as permanent by the caller
	}
	_, execErr := breaker.Execute(func() (any, error) {
		return nil, p.Send(ctx, e)
	})
	return execErr
}

// providerFor returns the cached provider + breaker for (client, vendor),
// building the provider from decrypted credentials on first use or after the
// credentials change.
func (r *Router) providerFor(clientID, vendor string, creds []byte) (provider.Provider, *gobreaker.CircuitBreaker[any], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.stateForLocked(clientID, vendor)
	if st.provider == nil || !bytes.Equal(st.credsRaw, creds) {
		plain := creds
		if r.cipher != nil && len(creds) > 0 {
			dec, err := r.cipher.DecryptCredentials(creds)
			if err != nil {
				return nil, nil, fmt.Errorf("decrypt %s credentials: %w", vendor, err)
			}
			plain = dec
		}
		prov, err := r.factories[vendor](plain)
		if err != nil {
			return nil, nil, fmt.Errorf("build %s provider: %w", vendor, err)
		}
		st.provider = prov
		st.credsRaw = append([]byte(nil), creds...)
	}
	return st.provider, st.breaker, nil
}

// stateForLocked returns the (client, vendor) state, creating its breaker and
// bucket on first reference. Caller holds r.mu.
func (r *Router) stateForLocked(clientID, vendor string) *vendorState {
	k := stateKey(clientID, vendor)
	st := r.states[k]
	if st == nil {
		st = &vendorState{
			breaker: gobreaker.NewCircuitBreaker[any](r.breakerSettings(vendor)),
			bucket:  &tokenBucket{tokens: r.capacity, capacity: r.capacity, refill: r.refill},
		}
		r.states[k] = st
	}
	return st
}

func (r *Router) breakerSettings(vendor string) gobreaker.Settings {
	minReqs := r.breakerMinReqs
	ratio := r.breakerRatio
	return gobreaker.Settings{
		Name:        vendor,
		MaxRequests: 1, // a single probe in half-open
		Timeout:     r.breakerTimeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			if c.Requests < minReqs {
				return false
			}
			return float64(c.TotalFailures)/float64(c.Requests) >= ratio
		},
		OnStateChange: func(name string, _, to gobreaker.State) {
			mBreakerState.WithLabelValues(name).Set(float64(int(to)))
		},
	}
}

// vendorOf extracts the vendor from a "clientID|vendor" state key.
func vendorOf(key string) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '|' {
			return key[i+1:]
		}
	}
	return key
}
