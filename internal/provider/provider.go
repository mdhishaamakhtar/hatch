// Package provider sends email through the vendors Hatch supports: Resend for
// real mail, and a mock with tunable latency and failure rates for benchmarks
// and the acceptance audit.
package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Vendors lists the vendors a client can register credentials for.
var Vendors = []string{"mock", "resend"}

// Email is one message to send.
type Email struct {
	To       string
	From     string
	FromName string
	Subject  string
	Body     string // HTML
}

// A Provider sends email through one vendor account.
type Provider interface {
	Send(ctx context.Context, e Email) error
}

// The Send errors worth retrying. Any other error is permanent.
var (
	ErrRateLimited = errors.New("provider rate limited")
	ErrTransient   = errors.New("provider transient error")
)

// New builds the Provider for vendor from a client's decrypted credentials. It
// is also how credentials are validated when a client registers them. mock
// tunes the mock vendor, which takes no credentials.
func New(vendor string, creds []byte, mock MockConfig) (Provider, error) {
	switch vendor {
	case "mock":
		return &mockProvider{cfg: mock}, nil
	case "resend":
		return newResend(creds)
	}
	return nil, fmt.Errorf("unsupported vendor %q", vendor)
}

// MockConfig tunes the mock vendor.
type MockConfig struct {
	Latency       time.Duration `env:"MOCK_PROVIDER_LATENCY"         envDefault:"150ms"`
	LatencyJitter time.Duration `env:"MOCK_PROVIDER_LATENCY_JITTER"  envDefault:"50ms"`
	ErrorRate     float64       `env:"MOCK_PROVIDER_ERROR_RATE"      envDefault:"0.001"`
	RateLimitRate float64       `env:"MOCK_PROVIDER_RATE_LIMIT_RATE" envDefault:"0"`

	// FailRecipient, when set, fails every send to that address transiently.
	// The acceptance audit uses it to walk a schedule through every retry tier.
	FailRecipient string `env:"MOCK_PROVIDER_FAIL_RECIPIENT"`
}

// mockProvider waits out a simulated latency and fails at the configured rates.
// Every send for a client shares one instance, which is safe because
// math/rand/v2's top-level functions are.
type mockProvider struct {
	cfg MockConfig
}

func (m *mockProvider) Send(ctx context.Context, e Email) error {
	latency := m.cfg.Latency
	if m.cfg.LatencyJitter > 0 {
		latency += rand.N(m.cfg.LatencyJitter)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(latency):
	}

	switch {
	case m.cfg.FailRecipient != "" && e.To == m.cfg.FailRecipient:
		return ErrTransient
	case rand.Float64() < m.cfg.RateLimitRate:
		return ErrRateLimited
	case rand.Float64() < m.cfg.ErrorRate:
		return ErrTransient
	}
	return nil
}
