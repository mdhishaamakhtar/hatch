// Package provider defines the email-provider abstraction Delivery Workers
// route sends through. Real provider implementations (Resend, SES, SendGrid,
// SMTP) land in Phase 3; Phase 0 ships only the interface and a MockProvider
// used by benchmarks.
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
}

// MockProvider satisfies Provider with env-controlled latency and error
// distributions. It performs no network I/O.
type MockProvider struct {
	cfg MockConfig
	rng *rand.Rand
}

// NewMockProvider returns a MockProvider seeded for reproducible probability
// draws within a single process. The seed is intentionally non-cryptographic.
func NewMockProvider(cfg MockConfig) *MockProvider {
	src := rand.NewPCG(uint64(time.Now().UnixNano()), 0xC0FFEE)
	return &MockProvider{cfg: cfg, rng: rand.New(src)}
}

// Vendor implements Provider.
func (m *MockProvider) Vendor() string { return "mock" }

// Send implements Provider with simulated latency and error rates.
func (m *MockProvider) Send(ctx context.Context, _ Email) error {
	jitter := 0
	if m.cfg.LatencyJitterMS > 0 {
		jitter = m.rng.IntN(m.cfg.LatencyJitterMS)
	}
	d := time.Duration(m.cfg.LatencyMS+jitter) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}

	if m.cfg.RateLimitRate > 0 && m.rng.Float64() < m.cfg.RateLimitRate {
		return ErrRateLimited
	}
	if m.cfg.ErrorRate > 0 && m.rng.Float64() < m.cfg.ErrorRate {
		return ErrTransient
	}
	return nil
}
