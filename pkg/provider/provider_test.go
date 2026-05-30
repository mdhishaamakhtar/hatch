package provider

import (
	"context"
	"errors"
	"testing"
)

func TestMockProvider_alwaysSucceeds(t *testing.T) {
	p := NewMockProvider(MockConfig{LatencyMS: 1, LatencyJitterMS: 0, ErrorRate: 0, RateLimitRate: 0})
	for i := range 50 {
		if err := p.Send(context.Background(), Email{}); err != nil {
			t.Fatalf("unexpected error on iter %d: %v", i, err)
		}
	}
}

func TestMockProvider_alwaysFails(t *testing.T) {
	p := NewMockProvider(MockConfig{LatencyMS: 1, LatencyJitterMS: 0, ErrorRate: 1.0, RateLimitRate: 0})
	if err := p.Send(context.Background(), Email{}); !errors.Is(err, ErrTransient) {
		t.Fatalf("err = %v, want ErrTransient", err)
	}
}

func TestMockProvider_alwaysRateLimited(t *testing.T) {
	p := NewMockProvider(MockConfig{LatencyMS: 1, LatencyJitterMS: 0, ErrorRate: 0, RateLimitRate: 1.0})
	if err := p.Send(context.Background(), Email{}); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", err)
	}
}

func TestMockProvider_failRecipient(t *testing.T) {
	// ErrorRate 0 + RateLimitRate 0 means the only way to fail is the sentinel.
	p := NewMockProvider(MockConfig{LatencyMS: 1, FailRecipient: "fail@mock.test"})

	if err := p.Send(context.Background(), Email{RecipientEmail: "fail@mock.test"}); !errors.Is(err, ErrTransient) {
		t.Fatalf("sentinel recipient: err = %v, want ErrTransient", err)
	}
	for i := range 50 {
		if err := p.Send(context.Background(), Email{RecipientEmail: "ok@example.com"}); err != nil {
			t.Fatalf("non-sentinel recipient should succeed (iter %d): %v", i, err)
		}
	}
}

func TestMockProvider_respectsCtx(t *testing.T) {
	p := NewMockProvider(MockConfig{LatencyMS: 1000, LatencyJitterMS: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Send(ctx, Email{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestVendor(t *testing.T) {
	p := NewMockProvider(MockConfig{})
	if p.Vendor() != "mock" {
		t.Errorf("Vendor = %q, want mock", p.Vendor())
	}
}
