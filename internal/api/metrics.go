package api

import (
	"slices"
	"strconv"

	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
)

// All hatch_api_* metrics. Registered exactly once at package init via the
// shared pkg/metrics registry so /metrics exposes them everywhere they exist.
var (
	mRequestsTotal = metrics.NewCounterVec(
		"api", "requests_total",
		"API requests by route pattern and status code.",
		"endpoint", "status_code",
	)
	mRequestDuration = metrics.NewHistogramVec(
		"api", "request_duration_seconds",
		"API request latency.",
		[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		"endpoint",
	)
	mValidationFailures = metrics.NewCounterVec(
		"api", "validation_failures_total",
		"Validation failures by reason.",
		"reason",
	)
	mIdempotencyHits = metrics.NewCounter(
		"api", "idempotency_hits_total",
		"Schedule creates that hit an existing idempotency key.",
	)
	// These two deliberately carry no client_id label. Per-client series are
	// unbounded in the number of clients, which is a Prometheus cardinality
	// incident waiting to happen; the per-client detail is already in the logs.
	mNoProviderRejections = metrics.NewCounter(
		"api", "no_provider_rejections_total",
		"Schedule creates rejected because the client has no active providers.",
	)
	mRateLimited = metrics.NewCounter(
		"api", "rate_limited_total",
		"Requests rejected by the per-client rate limiter.",
	)
)

// observeRequest is invoked once per request by the obs middleware.
func observeRequest(endpoint string, statusCode int, durationSec float64) {
	mRequestsTotal.WithLabelValues(endpoint, httpStatusLabel(statusCode)).Inc()
	mRequestDuration.WithLabelValues(endpoint).Observe(durationSec)
}

// exactStatusCodes are the 4xx codes Hatch actually returns. They're worth
// keeping precise on the metric; every other 4xx collapses to "4xx" so a client
// probing random codes can't blow up label cardinality.
var exactStatusCodes = []int{400, 401, 403, 404, 409, 413, 415, 422, 429}

// httpStatusLabel buckets a status code into the status_code metric label.
func httpStatusLabel(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		if slices.Contains(exactStatusCodes, code) {
			return strconv.Itoa(code)
		}
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}
