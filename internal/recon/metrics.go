package recon

import "github.com/mdhishaamakhtar/hatch/pkg/metrics"

// Metric vectors for the reconciliation cron. Names follow the project's hatch_*
// namespace and the Build Plan §Reconciliation Instrumentation: rows recovered
// per pass, run duration, and the last-run timestamp the staleness alert watches.
var (
	mRowsRecovered = metrics.NewCounter(
		"recon", "rows_recovered_total",
		"Stuck rows recovered by reconciliation and re-enqueued to emails.due, by pass.",
		"pass", // pass1 | pass2
	)
	mRunDuration = metrics.NewHistogram(
		"recon", "run_duration_seconds",
		"Wall time of one reconciliation run.",
		[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	)
	mLastRun = metrics.NewGauge(
		"recon", "last_run_timestamp",
		"Unix timestamp of the last successful reconciliation run; the staleness alert watches it.",
	)
)
