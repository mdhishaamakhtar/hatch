package archival

import "github.com/mdhishaamakhtar/hatch/pkg/metrics"

// Metric vectors for the partition archival cron. Names follow the project's
// hatch_* namespace and the Build Plan §Partition Archival Instrumentation. The
// active-partitions gauge is a DB-level property surfaced here because this
// service is what changes it.
var (
	mActivePartitions = metrics.NewGauge(
		"db", "active_partitions",
		"Number of partitions currently attached to scheduled_emails; refreshed after each archival run.",
	)
	mArchived = metrics.NewCounter(
		"archival", "partitions_archived_total",
		"Partitions archived (detached, exported, dropped) across all runs.",
	)
	mRunDuration = metrics.NewHistogram(
		"archival", "run_duration_seconds",
		"Wall time of one archival run.",
		[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	)
	mLastRun = metrics.NewGauge(
		"archival", "last_run_timestamp",
		"Unix timestamp of the last successful archival run; the staleness alert watches it.",
	)
)
