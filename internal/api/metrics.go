package api

import (
	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// All hatch_api_* metrics. Registered exactly once at package init via the
// shared pkg/metrics registry so /metrics exposes them everywhere they exist.
var (
	mRequestsTotal = metrics.NewCounter(
		"api", "requests_total",
		"API requests by route pattern and status code.",
		"endpoint", "status_code",
	)
	mRequestDuration = metrics.NewHistogram(
		"api", "request_duration_seconds",
		"API request latency.",
		[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		"endpoint",
	)
	mValidationFailures = metrics.NewCounter(
		"api", "validation_failures_total",
		"Validation failures by reason.",
		"reason",
	)
	mIdempotencyHits = metrics.NewCounter(
		"api", "idempotency_hits_total",
		"Schedule creates that hit an existing idempotency key.",
	)
	mNoProviderRejections = metrics.NewCounter(
		"api", "no_provider_rejections_total",
		"Schedule creates rejected because the client has no active providers.",
		"client_id",
	)
	mRateLimited = metrics.NewCounter(
		"api", "rate_limited_total",
		"Requests rejected by the per-client rate limiter.",
		"client_id",
	)
)

// observeRequest is invoked once per request by the obs middleware.
func observeRequest(endpoint string, statusCode int, durationSec float64) {
	mRequestsTotal.With(prometheus.Labels{
		"endpoint":    endpoint,
		"status_code": httpStatusLabel(statusCode),
	}).Inc()
	mRequestDuration.With(prometheus.Labels{"endpoint": endpoint}).Observe(durationSec)
}

func httpStatusLabel(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return statusExact(code)
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func statusExact(code int) string {
	// 4xx codes are low-cardinality and useful to keep precise (401, 404, 409, 422, 429).
	switch code {
	case 400:
		return "400"
	case 401:
		return "401"
	case 403:
		return "403"
	case 404:
		return "404"
	case 409:
		return "409"
	case 413:
		return "413"
	case 415:
		return "415"
	case 422:
		return "422"
	case 429:
		return "429"
	default:
		return "4xx"
	}
}
