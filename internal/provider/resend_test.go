package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"net/url"
	"strings"
	"testing"

	"github.com/resend/resend-go/v2"
)

// resendServer points a resend provider at a test server that answers every
// request with status and records the request body.
func resendServer(t *testing.T, status int) (*resendProvider, *map[string]any) {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"id":"abc","message":"test"}`))
	}))
	t.Cleanup(srv.Close)

	c := resend.NewClient("re_test")
	c.BaseURL, _ = url.Parse(srv.URL + "/")
	return &resendProvider{client: c}, &got
}

func TestResendErrorClassification(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{http.StatusOK, nil},
		{http.StatusTooManyRequests, ErrRateLimited},
		{http.StatusInternalServerError, ErrTransient},
		{http.StatusUnprocessableEntity, ErrTransient},
	}
	for _, c := range cases {
		p, _ := resendServer(t, c.status)
		err := p.Send(context.Background(), Email{To: "to@example.com", From: "from@example.com", Subject: "s", Body: "b"})
		if !errors.Is(err, c.want) {
			t.Errorf("HTTP %d: Send = %v, want %v", c.status, err, c.want)
		}
	}
}

// However hostile the display name, From must stay a single mailbox with no
// line breaks, and still carry the name the client asked for.
func TestResendFromHeaderCannotBeInjected(t *testing.T) {
	for _, name := range []string{"", "Hatch", "Evil <x@evil.com>, y@evil.com", "Line\r\nBcc: x@evil.com"} {
		p, got := resendServer(t, http.StatusOK)
		if err := p.Send(context.Background(), Email{To: "to@example.com", From: "from@example.com", FromName: name}); err != nil {
			t.Fatal(err)
		}
		from, _ := (*got)["from"].(string)
		addr, err := mail.ParseAddress(from)
		if err != nil || addr.Address != "from@example.com" || addr.Name != name || strings.ContainsAny(from, "\r\n") {
			t.Errorf("name %q produced From %q (parsed %+v, %v)", name, from, addr, err)
		}
	}
}
