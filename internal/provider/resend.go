package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"

	"github.com/resend/resend-go/v3"
)

// resendProvider sends through the Resend API with one client's own API key.
type resendProvider struct {
	client *resend.Client
}

func newResend(creds []byte) (Provider, error) {
	var c struct {
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(creds, &c); err != nil {
		return nil, fmt.Errorf("resend credentials: %w", err)
	}
	if c.APIKey == "" {
		return nil, errors.New("resend credentials: api_key is required")
	}
	return &resendProvider{client: resend.NewClient(c.APIKey)}, nil
}

// Send reports rate limiting as ErrRateLimited and every other failure as
// ErrTransient: Resend's SDK does not expose status codes, so a permanent
// failure (a revoked key, say) cannot be told apart and simply runs out of
// retries.
func (p *resendProvider) Send(ctx context.Context, e Email) error {
	from := e.From
	if e.FromName != "" {
		// net/mail quotes and encodes the display name, so it cannot smuggle
		// extra addresses or header lines into From.
		from = (&mail.Address{Name: e.FromName, Address: e.From}).String()
	}
	_, err := p.client.Emails.SendWithContext(ctx, &resend.SendEmailRequest{
		From:    from,
		To:      []string{e.To},
		Subject: e.Subject,
		Html:    e.Body,
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, resend.ErrRateLimit):
		return fmt.Errorf("%w: %v", ErrRateLimited, err)
	default:
		return fmt.Errorf("%w: %v", ErrTransient, err)
	}
}
