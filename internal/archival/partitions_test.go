package archival

import (
	"testing"
	"time"
)

func TestParsePartitionMonth(t *testing.T) {
	cases := []struct {
		name      string
		wantYear  int
		wantMonth int
		wantOK    bool
	}{
		{"scheduled_emails_y2026m05", 2026, 5, true},
		{"scheduled_emails_y2020m01", 2020, 1, true},
		{"scheduled_emails_y2026m12", 2026, 12, true},
		{"scheduled_emails", 0, 0, false},          // parent table
		{"schedule_idempotency", 0, 0, false},      // side table
		{"scheduled_emails_y2026m13", 0, 0, false}, // month out of range
		{"scheduled_emails_y2026m00", 0, 0, false}, // month out of range
		{"scheduled_emails_y26m5", 0, 0, false},    // wrong digit widths
		{"scheduled_emails_y2026m05x", 0, 0, false},
		{"", 0, 0, false},
	}
	for _, c := range cases {
		y, m, ok := parsePartitionMonth(c.name)
		if ok != c.wantOK || y != c.wantYear || m != c.wantMonth {
			t.Errorf("parsePartitionMonth(%q) = (%d,%d,%v), want (%d,%d,%v)",
				c.name, y, m, ok, c.wantYear, c.wantMonth, c.wantOK)
		}
	}
}

func TestIsFullyPast(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		year, month int
		want        bool
	}{
		{2026, 4, true},  // April fully elapsed
		{2020, 1, true},  // long past
		{2025, 12, true}, // prior December
		{2026, 5, false}, // current month — not fully past
		{2026, 6, false}, // next month
		{2027, 1, false}, // future
	}
	for _, c := range cases {
		if got := isFullyPast(c.year, c.month, now); got != c.want {
			t.Errorf("isFullyPast(%d,%d) = %v, want %v", c.year, c.month, got, c.want)
		}
	}
}

func TestIsFullyPastDecemberBoundary(t *testing.T) {
	// A December partition is fully past only once the following January begins.
	if isFullyPast(2025, 12, time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)) {
		t.Error("2025-12 should not be fully past on 2025-12-31")
	}
	if !isFullyPast(2025, 12, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("2025-12 should be fully past on 2026-01-01")
	}
}

func TestArchivePath(t *testing.T) {
	got := archivePath("/archive", "scheduled_emails_y2020m01")
	want := "/archive/scheduled_emails_y2020m01.csv.gz"
	if got != want {
		t.Errorf("archivePath = %q, want %q", got, want)
	}
}
