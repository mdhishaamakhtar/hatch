package delivery

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/internal/crypto"
	"github.com/mdhishaamakhtar/hatch/internal/provider"
	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

// Why the router could not send. errBreakerOpen and errNoCapacity are
// transient; their messages double as metric labels.
var (
	errNoProvider  = errors.New("no_active_providers")
	errBreakerOpen = errors.New("provider_breaker_open")
	errNoCapacity  = errors.New("provider_no_capacity")
)

// router spreads a client's sends across its providers, shielding each
// (client, vendor) pair behind a rate limit and a circuit breaker.
type router struct {
	mu     sync.Mutex
	routes map[routeKey]*route

	// newProvider builds a client's provider from its sealed credentials.
	newProvider func(clientID uuid.UUID, vendor string, sealed []byte) (provider.Provider, error)
	limit       rate.Limit // sends per second per route
	breaker     gobreaker.Settings
}

type routeKey struct {
	client uuid.UUID
	vendor string
}

// A route is one client's use of one vendor.
type route struct {
	limiter  *rate.Limiter
	breaker  *gobreaker.CircuitBreaker[struct{}]
	provider provider.Provider
	sealed   []byte // the credentials provider was built from
}

func newRouter(cipher *crypto.Cipher, mock provider.MockConfig) *router {
	return &router{
		routes: make(map[routeKey]*route),
		newProvider: func(clientID uuid.UUID, vendor string, sealed []byte) (provider.Provider, error) {
			creds, err := cipher.Decrypt(sealed, clientID[:], vendor)
			if err != nil {
				return nil, err
			}
			return provider.New(vendor, creds, mock)
		},
		limit: 1000,
		breaker: gobreaker.Settings{
			MaxRequests: 1, // one probe while half-open
			Timeout:     30 * time.Second,
			ReadyToTrip: func(c gobreaker.Counts) bool {
				return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
			},
			OnStateChange: func(vendor string, _, to gobreaker.State) {
				breakerState.WithLabelValues(vendor).Set(float64(to))
			},
		},
	}
}

// send sends e through one of the client's providers and returns the vendor it
// used. It prefers a provider other than lastFailed, the one that failed this
// schedule last time, but falls back to it rather than strand a client that
// has only one. Among the rest it picks the one with the most rate-limit
// headroom, skipping any whose breaker is open.
func (r *router) send(ctx context.Context, clientID uuid.UUID, providers []cachedProvider, lastFailed string, e provider.Email) (string, error) {
	p, rt, err := r.pick(clientID, providers, lastFailed)
	if err != nil {
		return "", err
	}
	prov, err := r.providerFor(clientID, p, rt)
	if err != nil {
		return p.Vendor, err
	}

	ctx, span := tracer.Start(ctx, "provider.send", trace.WithAttributes(attribute.String("provider", p.Vendor)))
	defer span.End()
	start := time.Now()
	_, err = rt.breaker.Execute(func() (struct{}, error) { return struct{}{}, prov.Send(ctx, e) })
	sendDuration.WithLabelValues(p.Vendor).Observe(time.Since(start).Seconds())
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		// The breaker opened, or its one half-open probe was taken, after pick.
		err = errBreakerOpen
	}
	if err != nil {
		span.RecordError(err)
	}
	return p.Vendor, err
}

// pick chooses a provider and takes a token from its rate limit.
func (r *router) pick(clientID uuid.UUID, providers []cachedProvider, lastFailed string) (cachedProvider, *route, error) {
	if len(providers) == 0 {
		return cachedProvider{}, nil, errNoProvider
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	allOpen := true
	for _, avoidLast := range []bool{true, false} {
		var best *route
		var bestProvider cachedProvider
		bestTokens := 0.0
		for _, p := range providers {
			if avoidLast && p.Vendor == lastFailed {
				continue
			}
			rt := r.route(clientID, p.Vendor)
			if rt.breaker.State() == gobreaker.StateOpen {
				continue
			}
			allOpen = false
			if tokens := rt.limiter.TokensAt(now); tokens >= 1 && tokens > bestTokens {
				best, bestProvider, bestTokens = rt, p, tokens
			}
		}
		if best != nil {
			best.limiter.AllowN(now, 1)
			return bestProvider, best, nil
		}
	}
	if allOpen {
		return cachedProvider{}, nil, errBreakerOpen
	}
	return cachedProvider{}, nil, errNoCapacity
}

// providerFor returns the route's provider, building it on first use and again
// whenever the client's credentials change.
func (r *router) providerFor(clientID uuid.UUID, p cachedProvider, rt *route) (provider.Provider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt.provider == nil || !bytes.Equal(rt.sealed, p.Credentials) {
		prov, err := r.newProvider(clientID, p.Vendor, p.Credentials)
		if err != nil {
			return nil, err
		}
		rt.provider, rt.sealed = prov, p.Credentials
	}
	return rt.provider, nil
}

// route returns the client's route to vendor, creating it on first use. The
// caller holds r.mu.
func (r *router) route(clientID uuid.UUID, vendor string) *route {
	key := routeKey{clientID, vendor}
	rt, ok := r.routes[key]
	if !ok {
		settings := r.breaker
		settings.Name = vendor
		rt = &route{
			limiter: rate.NewLimiter(r.limit, int(r.limit)), // a second's worth of burst
			breaker: gobreaker.NewCircuitBreaker[struct{}](settings),
		}
		r.routes[key] = rt
	}
	return rt
}
