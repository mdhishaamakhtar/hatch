package api

import (
	"testing"
	"time"
)

const (
	testMinHorizon = time.Hour
	testMaxHorizon = 24 * time.Hour
)

func TestValidateCreateSchedule(t *testing.T) {
	valid := createScheduleRequest{
		DeliverAt:      time.Now().Add(2 * time.Hour).UnixMilli(),
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
		{"valid", func(*createScheduleRequest) {}, ""},
		{"deliver_at missing", func(r *createScheduleRequest) { r.DeliverAt = 0 }, "deliver_at_required"},
		{"deliver_at negative", func(r *createScheduleRequest) { r.DeliverAt = -1 }, "deliver_at_format"},
		{"deliver_at inside the minimum horizon", func(r *createScheduleRequest) {
			r.DeliverAt = time.Now().Add(30 * time.Minute).UnixMilli()
		}, "deliver_at_too_soon"},
		// Past the partition runway the INSERT fails deep in Postgres, so this
		// has to be caught here as a 400 rather than surfacing as a 500.
		{"deliver_at past the maximum horizon", func(r *createScheduleRequest) {
			r.DeliverAt = time.Now().Add(48 * time.Hour).UnixMilli()
		}, "deliver_at_too_far"},
		{"recipient not an address", func(r *createScheduleRequest) { r.RecipientEmail = "nope" }, "recipient_email_invalid"},
		{"from not an address", func(r *createScheduleRequest) { r.FromEmail = "nope" }, "from_email_invalid"},
		{"subject empty", func(r *createScheduleRequest) { r.Subject = "" }, "subject_required"},
		{"body empty", func(r *createScheduleRequest) { r.Body = "" }, "body_required"},
		{"idempotency key too long", func(r *createScheduleRequest) {
			r.IdempotencyKey = string(make([]byte, maxIdempotencyKeyLen+1))
		}, "idempotency_key_too_long"},
		{"metadata too large", func(r *createScheduleRequest) {
			r.Metadata = make([]byte, maxMetadataBytes+1)
		}, "metadata_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := valid
			tc.mut(&in)
			if got := validateCreateSchedule(in, testMinHorizon, testMaxHorizon); got != tc.want {
				t.Fatalf("validateCreateSchedule = %q, want %q", got, tc.want)
			}
		})
	}
}

// Exactly-at-the-boundary values are accepted; the horizons are inclusive.
func TestValidateCreateScheduleHorizonBoundaries(t *testing.T) {
	base := createScheduleRequest{
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "hi",
		Body:           "<p>hi</p>",
	}
	// A small margin absorbs the clock advancing between building the request
	// and validating it.
	const margin = time.Second

	atMin := base
	atMin.DeliverAt = time.Now().Add(testMinHorizon + margin).UnixMilli()
	if got := validateCreateSchedule(atMin, testMinHorizon, testMaxHorizon); got != "" {
		t.Errorf("just inside the minimum horizon should be valid, got %q", got)
	}

	atMax := base
	atMax.DeliverAt = time.Now().Add(testMaxHorizon - margin).UnixMilli()
	if got := validateCreateSchedule(atMax, testMinHorizon, testMaxHorizon); got != "" {
		t.Errorf("just inside the maximum horizon should be valid, got %q", got)
	}
}

func TestHTTPStatusLabelKeepsCardinalityBounded(t *testing.T) {
	cases := map[int]string{
		101: "1xx",
		200: "2xx",
		301: "3xx",
		// The 4xx codes Hatch actually returns stay precise...
		400: "400", 401: "401", 403: "403", 404: "404",
		409: "409", 413: "413", 415: "415", 422: "422", 429: "429",
		// ...everything else collapses, so a client probing random codes can't
		// create unbounded series.
		418: "4xx",
		499: "4xx",
		500: "5xx",
		503: "5xx",
	}
	for code, want := range cases {
		if got := httpStatusLabel(code); got != want {
			t.Errorf("httpStatusLabel(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestOptionalStringMapsEmptyToNil(t *testing.T) {
	// An empty optional field must become SQL NULL, not an empty string.
	if optionalString("") != nil {
		t.Error("empty string should produce nil")
	}
	if v := optionalString("x"); v == nil || *v != "x" {
		t.Error("non-empty string should produce a pointer to the value")
	}
}
