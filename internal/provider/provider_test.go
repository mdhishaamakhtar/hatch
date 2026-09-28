package provider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func sendMock(cfg MockConfig, to string) error {
	cfg.Latency = time.Millisecond
	p, _ := New("mock", nil, cfg)
	return p.Send(context.Background(), Email{To: to})
}

func TestMockOutcomes(t *testing.T) {
	cases := []struct {
		name string
		cfg  MockConfig
		to   string
		want error
	}{
		{"healthy", MockConfig{}, "a@example.com", nil},
		{"always failing", MockConfig{ErrorRate: 1}, "a@example.com", ErrTransient},
		{"always rate limited", MockConfig{RateLimitRate: 1}, "a@example.com", ErrRateLimited},
		{"fail recipient", MockConfig{FailRecipient: "fail@mock.test"}, "fail@mock.test", ErrTransient},
		{"other recipients unaffected", MockConfig{FailRecipient: "fail@mock.test"}, "ok@example.com", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := sendMock(c.cfg, c.to); !errors.Is(err, c.want) {
				t.Fatalf("Send = %v, want %v", err, c.want)
			}
		})
	}
}

func TestMockStopsWhenTheContextIsCancelled(t *testing.T) {
	p, _ := New("mock", nil, MockConfig{Latency: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Send(ctx, Email{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send = %v, want context.Canceled", err)
	}
}

func TestNewValidatesVendorAndCredentials(t *testing.T) {
	cases := []struct {
		vendor, creds string
		ok            bool
	}{
		{"mock", `{}`, true},
		{"resend", `{"api_key":"re_123"}`, true},
		{"resend", `{"api_key":""}`, false},
		{"resend", `not json`, false},
		{"sendgrid", `{}`, false},
	}
	for _, c := range cases {
		_, err := New(c.vendor, []byte(c.creds), MockConfig{})
		if (err == nil) != c.ok {
			t.Errorf("New(%q, %s) error = %v, want ok=%v", c.vendor, c.creds, err, c.ok)
		}
	}
}
