package api

import (
	"testing"
	"time"
)

func TestValidateCreateSchedule(t *testing.T) {
	future := time.Now().Add(2 * time.Hour).UnixMilli()
	near := time.Now().Add(30 * time.Minute).UnixMilli()
	base := createScheduleRequest{
		DeliverAt:      future,
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "hi",
		Body:           "<p>hi</p>",
	}

	cases := []struct {
		name string
		mut  func(*createScheduleRequest)
		want string
	}{
		{"ok", func(*createScheduleRequest) {}, ""},
		{"deliver_at zero", func(r *createScheduleRequest) { r.DeliverAt = 0 }, "deliver_at_required"},
		{"deliver_at negative", func(r *createScheduleRequest) { r.DeliverAt = -1 }, "deliver_at_format"},
		{"deliver_at too soon", func(r *createScheduleRequest) { r.DeliverAt = near }, "deliver_at_too_soon"},
		{"recipient invalid", func(r *createScheduleRequest) { r.RecipientEmail = "nope" }, "recipient_email_invalid"},
		{"from invalid", func(r *createScheduleRequest) { r.FromEmail = "nope" }, "from_email_invalid"},
		{"subject empty", func(r *createScheduleRequest) { r.Subject = "" }, "subject_required"},
		{"body empty", func(r *createScheduleRequest) { r.Body = "" }, "body_required"},
		{"idempotency long", func(r *createScheduleRequest) {
			r.IdempotencyKey = string(make([]byte, 300))
		}, "idempotency_key_too_long"},
		{"metadata huge", func(r *createScheduleRequest) {
			r.Metadata = make([]byte, 9*1024)
		}, "metadata_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			tc.mut(&in)
			got := validateCreateSchedule(in)
			if got != tc.want {
				t.Fatalf("validateCreateSchedule: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestHTTPStatusLabel(t *testing.T) {
	cases := map[int]string{
		200: "2xx",
		301: "3xx",
		400: "400",
		401: "401",
		404: "404",
		409: "409",
		413: "413",
		415: "415",
		422: "422",
		429: "429",
		499: "4xx",
		500: "5xx",
		503: "5xx",
		101: "1xx",
	}
	for code, want := range cases {
		if got := httpStatusLabel(code); got != want {
			t.Errorf("httpStatusLabel(%d) = %q want %q", code, got, want)
		}
	}
}

func TestOptionalString(t *testing.T) {
	if optionalString("") != nil {
		t.Fatal("empty string should produce nil")
	}
	v := optionalString("x")
	if v == nil || *v != "x" {
		t.Fatal("non-empty string should produce pointer to value")
	}
}
