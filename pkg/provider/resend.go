package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/resend/resend-go/v2"
)

// resendCreds is the decrypted credential shape for vendor "resend".
type resendCreds struct {
	APIKey string `json:"api_key"`
}

// ResendProvider sends email through the Resend HTTP API. One instance is built
// per client from that client's API key, so two clients sending via Resend each
// use their own key.
type ResendProvider struct {
	client *resend.Client
}

// ResendFactory parses {"api_key":"re_..."} from the decrypted credentials and
// returns a Resend-backed Provider. Registered for vendor "resend".
func ResendFactory(creds []byte) (Provider, error) {
	var c resendCreds
	if err := json.Unmarshal(creds, &c); err != nil {
		return nil, fmt.Errorf("resend credentials: %w", err)
	}
	if c.APIKey == "" {
		return nil, fmt.Errorf("resend credentials missing api_key")
	}
	return &ResendProvider{client: resend.NewClient(c.APIKey)}, nil
}

// Vendor implements Provider.
func (p *ResendProvider) Vendor() string { return "resend" }

// Send implements Provider via Resend's SendWithContext.
//
// Error classification: Resend's Go SDK only types the 429 case
// (resend.ErrRateLimit); every other non-2xx (4xx auth/validation and 5xx alike)
// collapses to an opaque error with no status code. We therefore map rate limits
// to ErrRateLimited and treat all other failures as ErrTransient (retryable). A
// genuinely permanent failure simply exhausts the retry budget and terminates as
// `failed` — the safe default given the SDK can't distinguish 4xx from 5xx.
func (p *ResendProvider) Send(ctx context.Context, e Email) error {
	req := &resend.SendEmailRequest{
		From:    formatFrom(e.FromName, e.FromEmail),
		To:      []string{e.RecipientEmail},
		Subject: e.Subject,
		Html:    e.Body,
	}
	if _, err := p.client.Emails.SendWithContext(ctx, req); err != nil {
		if errors.Is(err, resend.ErrRateLimit) {
			return fmt.Errorf("%w: %v", ErrRateLimited, err)
		}
		return fmt.Errorf("%w: %v", ErrTransient, err)
	}
	return nil
}

// formatFrom renders the RFC 5322 From header. With a display name it becomes
// "Name <addr>"; otherwise just the bare address.
func formatFrom(name, email string) string {
	if name == "" {
		return email
	}
	return fmt.Sprintf("%s <%s>", name, email)
}
