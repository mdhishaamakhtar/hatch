package delivery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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

// stateKeySep joins client id and vendor into a states map key. Neither part
// can contain it: client ids are UUIDs and vendors come from a fixed allowlist.
const stateKeySep = '|'

func stateKey(clientID, vendor string) string {
	return clientID + string(stateKeySep) + vendor
}

// Refill tops up every bucket. Called by G3 on each tick.
func (r *Router) Refill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, st := range r.states {
		st.bucket.topUp()
		mBucketTokens.WithLabelValues(vendorOf(k)).Set(float64(st.bucket.tokens))
	}
}

// Selection is the outcome of a Select call. Outcome tells the processor which
// of three very different situations it is in; Vendor and Creds are only set
// when Outcome is SelectOK.
type Selection struct {
	Outcome SelectOutcome
	Vendor  string
	Creds   []byte
}

// SelectOutcome distinguishes the reasons a send may not proceed. They demand
// opposite responses, so collapsing them into a single bool is what turns a
// vendor blip or a burst of traffic into a permanently failed email.
type SelectOutcome int

const (
	// SelectOK — a vendor was chosen and a token consumed.
	SelectOK SelectOutcome = iota
	// SelectNoEligibleVendor — the client has no provider with a registered
	// implementation. A configuration problem, and genuinely terminal.
	SelectNoEligibleVendor
	// SelectBreakerOpen — every candidate's circuit breaker is OPEN. A transient
	// vendor outage: route to the retry tiers, don't fail the email.
	SelectBreakerOpen
	// SelectNoCapacity — candidates exist and are healthy, but their leaky
	// buckets are empty. Pure backpressure: wait for a refill and reselect.
	SelectNoCapacity
)

// Select runs the LLD selection algorithm and consumes one token from the chosen
// vendor's bucket.
//
// last_provider exclusion is best-effort: it avoids immediately re-hitting the
// vendor that just failed *when an alternative exists*. If excluding it would
// leave no candidate — i.e. it's the client's only eligible provider — the
// exclusion is dropped and we retry on it anyway. The retry tiers exist to
// reattempt transient failures, so a single-provider client must not be stranded
// with no_active_providers after one transient blip; a genuinely unhealthy sole
// provider still trips its breaker and yields SelectBreakerOpen.
func (r *Router) Select(clientID string, providers []cachedProvider, lastProvider string) Selection {
	r.mu.Lock()
	defer r.mu.Unlock()

	// First pass: prefer any healthy vendor other than the one that just failed.
	pick := r.pickBestLocked(clientID, providers, lastProvider)
	if pick.state == nil && lastProvider != "" {
		// The just-failed vendor is the only eligible option — retry on it rather
		// than failing the send for lack of an alternative.
		pick = r.pickBestLocked(clientID, providers, "")
	}
	switch {
	case pick.state == nil:
		return Selection{Outcome: pick.emptyOutcome()}
	case !pick.state.bucket.take():
		return Selection{Outcome: SelectNoCapacity}
	}
	mBucketTokens.WithLabelValues(pick.vendor).Set(float64(pick.state.bucket.tokens))
	return Selection{Outcome: SelectOK, Vendor: pick.vendor, Creds: pick.creds}
}

// pick is pickBestLocked's result: the winning vendor (state nil if none), plus
// enough detail to say *why* nothing won.
type pick struct {
	vendor       string
	creds        []byte
	state        *vendorState
	sawCandidate bool // a vendor with a registered implementation existed
	sawOpen      bool // at least one such vendor was skipped for an OPEN breaker
}

// emptyOutcome classifies a pick that found no vendor.
func (p pick) emptyOutcome() SelectOutcome {
	switch {
	case p.sawOpen:
		return SelectBreakerOpen
	case p.sawCandidate:
		// Candidates existed and were closed, so the only way none won the
		// highest-tokens comparison is that they all sat at zero tokens.
		return SelectNoCapacity
	default:
		return SelectNoEligibleVendor
	}
}

// pickBestLocked scans providers for the highest-token vendor that has a
// registered implementation and a non-OPEN breaker, optionally skipping
// excludeVendor. It does not consume a token — the caller takes one from the
// returned state. Caller holds r.mu.
func (r *Router) pickBestLocked(clientID string, providers []cachedProvider, excludeVendor string) pick {
	var out pick
	bestTokens := 0
	for _, p := range providers {
		if _, has := r.factories[p.Vendor]; !has {
			continue // no implementation registered for this vendor
		}
		if excludeVendor != "" && p.Vendor == excludeVendor {
			continue // skip the vendor that just failed (when an alternative exists)
		}
		out.sawCandidate = true
		st := r.stateForLocked(clientID, p.Vendor)
		if st.breaker.State() == gobreaker.StateOpen {
			out.sawOpen = true
			continue
		}
		if st.bucket.tokens > bestTokens {
			bestTokens = st.bucket.tokens
			out.state = st
			out.vendor = p.Vendor
			out.creds = p.Credentials
		}
	}
	return out
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
	if i := strings.LastIndexByte(key, stateKeySep); i >= 0 {
		return key[i+1:]
	}
	return key
}
