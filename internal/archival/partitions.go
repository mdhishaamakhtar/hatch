package archival

import (
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// partitionNameRE matches the scheduled_emails partition naming convention from
// migration 004: scheduled_emails_yYYYYmMM (e.g. scheduled_emails_y2026m05).
var partitionNameRE = regexp.MustCompile(`^scheduled_emails_y(\d{4})m(\d{2})$`)

// parsePartitionMonth extracts the (year, month) a scheduled_emails partition
// covers from its name. ok is false for names that don't match the convention
// (the parent table, side tables, or anything unrelated) so callers skip them.
func parsePartitionMonth(name string) (year, month int, ok bool) {
	m := partitionNameRE.FindStringSubmatch(name)
	if m == nil {
		return 0, 0, false
	}
	year, _ = strconv.Atoi(m[1])
	month, _ = strconv.Atoi(m[2])
	if month < 1 || month > 12 {
		return 0, 0, false
	}
	return year, month, true
}

// isFullyPast reports whether the (year, month) has fully elapsed as of now —
// i.e. the first instant of the following month is at or before now. Only
// fully-past partitions are eligible for archival; the current month and the
// future runway are never touched. time.Date normalises month 13 to January of
// the next year, so December is handled correctly.
func isFullyPast(year, month int, now time.Time) bool {
	nextMonth := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
	return !nextMonth.After(now.UTC())
}

// archivePath is the on-disk destination for a partition's gzip CSV export.
func archivePath(dir, name string) string {
	return filepath.Join(dir, name+".csv.gz")
}
