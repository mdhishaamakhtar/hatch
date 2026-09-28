package delivery

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics labelled by provider carry the vendor only, not the client: a series
// per client would grow without bound.
var (
	batchSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_delivery_batch_size",
		Help:    "Records per batch read from emails.due.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2000},
	})
	batchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_delivery_batch_duration_seconds",
		Help:    "Time to process one batch from emails.due.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})
	e2eLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_delivery_e2e_latency_seconds",
		Help:    "Time from a schedule's deliver_at to its successful send.",
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	})
	sends = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_sends_total",
		Help: "Provider send attempts by outcome: success, transient, rate_limited, permanent_error or aborted.",
	}, []string{"provider", "status"})
	sendDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hatch_delivery_provider_send_duration_seconds",
		Help:    "Latency of one provider send.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"provider"})
	cacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_client_cache_total",
		Help: "Client cache lookups by result: hit, miss or unavailable.",
	}, []string{"result"})
	idempotency = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_idempotency_total",
		Help: "Send claims by result: acquired, duplicate_sent, duplicate_in_flight or unavailable.",
	}, []string{"result"})
	skipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_skipped_total",
		Help: "Records dropped because their schedule had already finished, by its status.",
	}, []string{"status"})
	deferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_deferred_total",
		Help: "Sends put off to a retry tier without contacting a provider, by reason.",
	}, []string{"reason"})
	lostRaces = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_status_write_lost_total",
		Help: "Status writes that changed nothing because the row had already moved.",
	}, []string{"attempted"})
	retries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_retries_total",
		Help: "Sends parked on a retry tier, by tier.",
	}, []string{"tier"})
	failed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_delivery_failed_total",
		Help: "Schedules that failed for good, by reason: no_active_providers, retry_exhausted or provider_error.",
	}, []string{"reason"})
	cancelled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_delivery_cancelled_total",
		Help: "Schedules cancelled at send time because their client had been deactivated.",
	})
	breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hatch_delivery_circuit_breaker_state",
		Help: "The last circuit breaker state change seen per vendor: 0 closed, 1 half-open, 2 open.",
	}, []string{"provider"})
)
