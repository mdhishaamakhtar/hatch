package archival

import (
	"testing"
	"time"
)

func TestMonthEnded(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		want bool
	}{
		{"scheduled_emails_y2026m04", true},
		{"scheduled_emails_y2020m01", true},
		{"scheduled_emails_y2025m12", true},
		{"scheduled_emails_y2026m05", false}, // this month
		{"scheduled_emails_y2026m06", false},
		{"scheduled_emails", false}, // the parent
		{"schedule_idempotency", false},
		{"scheduled_emails_y2026m13", false},
		{"scheduled_emails_y26m5", false},
		{"scheduled_emails_y2026m04x", false},
		{"public.scheduled_emails_y2026m04", false},
	}
	for _, c := range cases {
		if got := monthEnded(c.name, now); got != c.want {
			t.Errorf("monthEnded(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// December's partition is over only once January has begun.
func TestMonthEndedAtTheYearBoundary(t *testing.T) {
	const december = "scheduled_emails_y2025m12"
	if monthEnded(december, time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)) {
		t.Error("December 2025 counted as over on its last day")
	}
	if !monthEnded(december, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("December 2025 not over on New Year's Day")
	}
}
