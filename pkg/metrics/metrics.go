// Package metrics holds the project-wide Prometheus registry and helpers.
//
// Every metric created via this package is namespaced `hatch_*` so Prometheus
// queries and the alerts defined in the Observability doc work uniformly.
//
// The constructors mirror the prometheus client's own naming: plain
// New{Counter,Gauge,Histogram} return unlabelled metrics, the *Vec variants take
// label names. Use the plain ones when a metric has no labels — they avoid a
// no-op `.WithLabelValues()` at every call site.
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

// NewCounter creates and registers an unlabelled counter in the hatch_ namespace.
func NewCounter(subsystem, name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	})
	Registry.MustRegister(c)
	return c
}

// NewCounterVec creates and registers a labelled counter in the hatch_ namespace.
func NewCounterVec(subsystem, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	}, labels)
	Registry.MustRegister(c)
	return c
}

// NewHistogram creates and registers an unlabelled histogram in the hatch_
// namespace. Nil buckets fall back to Prometheus's defaults.
func NewHistogram(subsystem, name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help, Buckets: buckets,
	})
	Registry.MustRegister(h)
	return h
}

// NewHistogramVec creates and registers a labelled histogram in the hatch_
// namespace. Nil buckets fall back to Prometheus's defaults.
func NewHistogramVec(subsystem, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help, Buckets: buckets,
	}, labels)
	Registry.MustRegister(h)
	return h
}

// NewGauge creates and registers an unlabelled gauge in the hatch_ namespace.
func NewGauge(subsystem, name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	})
	Registry.MustRegister(g)
	return g
}

// NewGaugeVec creates and registers a labelled gauge in the hatch_ namespace.
func NewGaugeVec(subsystem, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help,
	}, labels)
	Registry.MustRegister(g)
	return g
}
