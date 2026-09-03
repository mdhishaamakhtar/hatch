// Package provider defines the email-provider abstraction delivery workers
// route sends through, plus the implementations behind it: Resend for real
// sends, and a MockProvider with env-tunable latency and error rates for
// benchmarks and the acceptance verifier.
//
// A vendor is wired in as a Factory — the delivery worker holds a
// map[vendor]Factory and builds a per-client Provider from that client's
// decrypted credentials on first send. Adding SES, SendGrid, or SMTP means
// adding a Factory here and an entry to that map; nothing else changes.
package provider

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Email is the payload passed to Send. Mirrors the persisted columns the
// Delivery Worker hydrates from Postgres.
type Email struct {
	ScheduleID     []byte
	ClientID       []byte
	RecipientEmail string
	FromEmail      string
	FromName       string
	Subject        string
	Body           string
}

// Provider is the interface every vendor implementation satisfies.
type Provider interface {
	// Vendor returns the canonical vendor name ("resend", "ses", "sendgrid",
	// "smtp", "mock") matching the client_providers.vendor column.
	Vendor() string
	// Send attempts to deliver e. A non-nil error means the send failed and the
	// caller should record retry state per the retry tier logic.
	Send(ctx context.Context, e Email) error
}

// ErrRateLimited is returned when a provider signals 429-style backpressure.
var ErrRateLimited = errors.New("provider rate limited")

// ErrTransient is returned when a provider call fails for a retryable reason.
var ErrTransient = errors.New("provider transient error")

// MockConfig tunes MockProvider behaviour from env-injected values. The
// defaults match the Benchmarking doc.
type MockConfig struct {
	LatencyMS       int     `env:"MOCK_PROVIDER_LATENCY_MS" envDefault:"150"`
	LatencyJitterMS int     `env:"MOCK_PROVIDER_LATENCY_JITTER_MS" envDefault:"50"`
	ErrorRate       float64 `env:"MOCK_PROVIDER_ERROR_RATE" envDefault:"0.001"`
	RateLimitRate   float64 `env:"MOCK_PROVIDER_RATE_LIMIT_RATE" envDefault:"0.0"`

	// FailRecipient, when set, makes Send return ErrTransient on every attempt
	// for that exact recipient — a deterministic failure seam used by the
	// acceptance verifier to drive a schedule through all three retry tiers
	// without touching the probabilistic ErrorRate. Empty (the default)
	// disables it, so it never affects normal traffic.
	FailRecipient string `env:"MOCK_PROVIDER_FAIL_RECIPIENT"`
}

// MockProvider satisfies Provider with env-controlled latency and error
// distributions. It performs no network I/O.
//
// One instance is shared by every concurrent send for a (client, vendor), so it
// must be safe for concurrent use. It draws from math/rand/v2's top-level
// functions, which are goroutine-safe; a *rand.Rand of its own would not be, and
// would race as soon as a worker sends more than one email at a time.
type MockProvider struct {
	cfg MockConfig
}

// NewMockProvider returns a MockProvider. Draws are non-cryptographic and not
// reproducible — with concurrent sends the interleaving is not repeatable
// anyway, so a fixed seed would promise something it could not deliver.
func NewMockProvider(cfg MockConfig) *MockProvider {
	return &MockProvider{cfg: cfg}
}

// Vendor implements Provider.
func (m *MockProvider) Vendor() string { return "mock" }

// Send implements Provider with simulated latency and error rates.
func (m *MockProvider) Send(ctx context.Context, e Email) error {
	jitter := 0
	if m.cfg.LatencyJitterMS > 0 {
		jitter = rand.IntN(m.cfg.LatencyJitterMS)
	}
	d := time.Duration(m.cfg.LatencyMS+jitter) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}

	// Deterministic failure seam: the sentinel recipient always fails transiently.
	if m.cfg.FailRecipient != "" && e.RecipientEmail == m.cfg.FailRecipient {
		return ErrTransient
	}

	if m.cfg.RateLimitRate > 0 && rand.Float64() < m.cfg.RateLimitRate {
		return ErrRateLimited
	}
	if m.cfg.ErrorRate > 0 && rand.Float64() < m.cfg.ErrorRate {
		return ErrTransient
	}
	return nil
}
