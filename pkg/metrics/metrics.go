// Package metrics holds the project-wide Prometheus registry and helpers.
//
// Every metric created via this package is namespaced `hatch_*` so Prometheus
// queries and the alerts defined in the Observability doc work uniformly.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace is the prefix applied to every metric. Matches the Observability doc.
const Namespace = "hatch"

// Registry is the package-global registry. Services register their metrics here.
var Registry = prometheus.NewRegistry()

// Handler exposes /metrics for Prometheus scraping.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}

// NewCounter creates and registers a counter in the hatch_ namespace.
func NewCounter(subsystem, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	}, labels)
	Registry.MustRegister(c)
	return c
}

// NewHistogram creates and registers a histogram in the hatch_ namespace.
// buckets nil falls back to Prometheus's default buckets.
func NewHistogram(subsystem, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help, Buckets: buckets,
	}, labels)
	Registry.MustRegister(h)
	return h
}

// NewGauge creates and registers a gauge in the hatch_ namespace.
func NewGauge(subsystem, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	}, labels)
	Registry.MustRegister(g)
	return g
}
