package api

import (
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	valid := createScheduleRequest{
		DeliverAt:      now.Add(2 * time.Hour).UnixMilli(),
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "hi",
		Body:           "<p>hi</p>",
	}

	cases := []struct {
		name string
		edit func(*createScheduleRequest)
		want string
	}{
		{"valid", func(*createScheduleRequest) {}, ""},
		{"deliver_at missing", func(r *createScheduleRequest) { r.DeliverAt = 0 }, "deliver_at_required"},
		{"deliver_at negative", func(r *createScheduleRequest) { r.DeliverAt = -1 }, "deliver_at_format"},
		{"deliver_at inside the horizon", func(r *createScheduleRequest) { r.DeliverAt = now.Add(59 * time.Minute).UnixMilli() }, "deliver_at_too_soon"},
		{"deliver_at exactly at the horizon", func(r *createScheduleRequest) { r.DeliverAt = now.Add(time.Hour).UnixMilli() }, ""},
		{"deliver_at past the partitions", func(r *createScheduleRequest) {
			r.DeliverAt = now.Add(maxScheduleHorizon + time.Hour).UnixMilli()
		}, "deliver_at_too_far"},
		{"recipient not an address", func(r *createScheduleRequest) { r.RecipientEmail = "nope" }, "recipient_email_invalid"},
		// A display name or a list would reach the provider as extra recipients.
		{"recipient with a display name", func(r *createScheduleRequest) { r.RecipientEmail = "Bob <bob@example.com>" }, "recipient_email_invalid"},
		{"recipient list", func(r *createScheduleRequest) { r.RecipientEmail = "a@example.com, b@example.com" }, "recipient_email_invalid"},
		{"from with a display name", func(r *createScheduleRequest) { r.FromEmail = "Evil <x@example.com>" }, "from_email_invalid"},
		{"subject empty", func(r *createScheduleRequest) { r.Subject = "" }, "subject_required"},
		{"body empty", func(r *createScheduleRequest) { r.Body = "" }, "body_required"},
		{"idempotency key too long", func(r *createScheduleRequest) {
			r.IdempotencyKey = strings.Repeat("k", maxIdempotencyKeyLen+1)
		}, "idempotency_key_too_long"},
		{"metadata too large", func(r *createScheduleRequest) { r.Metadata = make([]byte, maxMetadataBytes+1) }, "metadata_too_large"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := valid
			c.edit(&in)
			if got := in.validate(now, time.Hour); got != c.want {
				t.Fatalf("validate = %q, want %q", got, c.want)
			}
		})
	}
}
