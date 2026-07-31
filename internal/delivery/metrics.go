package delivery

import "github.com/mdhishaamakhtar/hatch/pkg/metrics"

// Metric vectors for the delivery worker. Names + labels follow the
// Observability doc (hatch_delivery_* in the project's `hatch` namespace).
//
// Per-(client,vendor) state (breaker, bucket) is collapsed to a `provider`
// (vendor) label to keep cardinality bounded; with the small client counts this
// phase targets, last-writer-wins on these gauges is acceptable.
var (
	mBatchSize = metrics.NewHistogram(
		"delivery", "batch_size",
		"Number of schedule ids fetched per emails.due batch.",
		[]float64{1, 10, 50, 100, 250, 500, 1000, 2000},
	)
	mBatchDuration = metrics.NewHistogram(
		"delivery", "batch_duration_seconds",
		"Wall time to process one emails.due batch.",
		[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	)
	mE2ELatency = metrics.NewHistogram(
		"delivery", "e2e_latency_seconds",
		"From scheduled deliver_at to successful delivery.",
		[]float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	)
	mSends = metrics.NewCounterVec(
		"delivery", "sends_total",
		"Provider send attempts by vendor and outcome.",
		"provider", "status", // success | transient | rate_limited | permanent_error
	)
	mSendDuration = metrics.NewHistogramVec(
		"delivery", "provider_send_duration_seconds",
		"Latency of a single provider Send call.",
		[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		"provider",
	)
	mCacheOps = metrics.NewCounterVec(
		"delivery", "client_cache_total",
		"Client cache lookups by result.",
		"result", // hit | miss | unavailable
	)
	mIdem = metrics.NewCounterVec(
		"delivery", "idempotency_total",
		"Redis idempotency claim results.",
		"result", // acquired | duplicate_sent | duplicate_in_flight | unavailable
	)
	mSkipped = metrics.NewCounterVec(
		"delivery", "skipped_total",
		"Records dropped because the row was already in a terminal state.",
		"status", // delivered | failed | cancelled
	)
	mDeferred = metrics.NewCounterVec(
		"delivery", "deferred_total",
		"Sends deferred to the retry tiers without a provider call, by reason.",
		"reason", // provider_breaker_open | provider_no_capacity
	)
	mLostRace = metrics.NewCounterVec(
		"delivery", "status_write_lost_total",
		"Guarded status writes that changed 0 rows because the row moved first.",
		"attempted", // processing | delivered | retrying | failed | cancelled
	)
	mRetries = metrics.NewCounterVec(
		"delivery", "retries_total",
		"Retry re-enqueues by tier.",
		"tier", // 1min | 5min | 30min
	)
	mFailed = metrics.NewCounterVec(
		"delivery", "failed_total",
		"Terminal failures by reason.",
		"reason", // no_active_providers | retry_exhausted | provider_error
	)
	mCancelled = metrics.NewCounterVec(
		"delivery", "cancelled_total",
		"Cancelled-during-delivery by reason.",
		"reason", // client_inactive
	)
	mBreakerState = metrics.NewGaugeVec(
		"delivery", "circuit_breaker_state",
		"Per-vendor circuit breaker state (0=closed, 1=half-open, 2=open).",
		"provider",
	)
	mBucketTokens = metrics.NewGaugeVec(
		"delivery", "leaky_bucket_tokens",
		"Available leaky-bucket tokens for a vendor (last observed).",
		"provider",
	)
)
