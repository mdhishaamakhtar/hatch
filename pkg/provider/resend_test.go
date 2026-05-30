package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/resend/resend-go/v2"
)

// resendTo builds a ResendProvider pointed at a test server.
func resendTo(t *testing.T, serverURL string) *ResendProvider {
	t.Helper()
	c := resend.NewClient("re_test_key")
	u, err := url.Parse(serverURL + "/")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	c.BaseURL = u
	return &ResendProvider{client: c}
}

func TestResendSendErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantErrIs  error // nil means success
		wantErrNil bool
	}{
		{name: "success", status: http.StatusOK, body: `{"id":"abc"}`, wantErrNil: true},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `{"message":"slow down"}`, wantErrIs: ErrRateLimited},
		{name: "server error transient", status: http.StatusInternalServerError, body: `{"message":"boom"}`, wantErrIs: ErrTransient},
		{name: "bad request transient", status: http.StatusUnprocessableEntity, body: `{"message":"bad from"}`, wantErrIs: ErrTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			p := resendTo(t, srv.URL)
			err := p.Send(context.Background(), Email{
				RecipientEmail: "to@example.com",
				FromEmail:      "from@nexia.hishaam.dev",
				Subject:        "hi",
				Body:           "<p>hi</p>",
			})

			if tc.wantErrNil {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("want errors.Is(_, %v), got %v", tc.wantErrIs, err)
			}
		})
	}
}

func TestResendVendor(t *testing.T) {
	if (&ResendProvider{}).Vendor() != "resend" {
		t.Fatal("vendor should be resend")
	}
}

func TestFormatFrom(t *testing.T) {
	if got := formatFrom("Hatch", "hi@nexia.hishaam.dev"); got != "Hatch <hi@nexia.hishaam.dev>" {
		t.Errorf("with name: got %q", got)
	}
	if got := formatFrom("", "hi@nexia.hishaam.dev"); got != "hi@nexia.hishaam.dev" {
		t.Errorf("without name: got %q", got)
	}
}

func TestResendFactory(t *testing.T) {
	p, err := ResendFactory([]byte(`{"api_key":"re_123"}`))
	if err != nil || p == nil {
		t.Fatalf("valid creds should build a provider: %v", err)
	}
	if _, err := ResendFactory([]byte(`{"api_key":""}`)); err == nil {
		t.Error("empty api_key should error")
	}
	if _, err := ResendFactory([]byte(`not json`)); err == nil {
		t.Error("malformed creds should error")
	}
}
