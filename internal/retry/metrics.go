package retry

import "github.com/mdhishaamakhtar/hatch/pkg/metrics"

// Metric vectors for the retry consumer. Names follow the project's hatch_*
// namespace and the Build Plan §Retry Consumer Instrumentation: drained total,
// re-enqueue failures, and drain duration — all labelled by tier.
var (
	mDrained = metrics.NewCounter(
		"retry", "drained_total",
		"Schedule ids drained from a retry tier and re-enqueued to emails.due.",
		"tier", // 1min | 5min | 30min
	)
	mReenqueueFailures = metrics.NewCounter(
		"retry", "reenqueue_failures_total",
		"Failed re-enqueues to emails.due, by tier.",
		"tier",
	)
	mDrainDuration = metrics.NewHistogram(
		"retry", "drain_duration_seconds",
		"Wall time of one tier drain cycle.",
		[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		"tier",
	)
)
