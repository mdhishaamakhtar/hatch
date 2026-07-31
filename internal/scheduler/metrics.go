package scheduler

import (
	"strconv"

	"github.com/mdhishaamakhtar/hatch/pkg/metrics"
)

// Metric vectors for the scheduler service. Names + labels match the
// Observability doc (hatch_scheduler_* in the project's `hatch` namespace).
var (
	mPollDuration = metrics.NewHistogramVec(
		"scheduler", "poll_duration_seconds",
		"Duration of one hourly Postgres poll cycle.",
		[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		"pod_index",
	)
	mPollEmailsLoaded = metrics.NewCounterVec(
		"scheduler", "poll_emails_loaded_total",
		"Schedule rows loaded into the wheel per poll cycle.",
		"pod_index",
	)
	mWheelOccupied = metrics.NewGaugeVec(
		"scheduler", "wheel_occupied_slots",
		"Number of second-slots currently holding ≥1 schedule id.",
		"pod_index",
	)
	mWheelTotalLoaded = metrics.NewGaugeVec(
		"scheduler", "wheel_total_loaded",
		"Total schedule ids currently in the wheel.",
		"pod_index",
	)
	mProduceDuration = metrics.NewHistogramVec(
		"scheduler", "kafka_produce_duration_seconds",
		"Latency of Kafka produce on emails.due.",
		[]float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		"pod_index",
	)
	mProduceFailures = metrics.NewCounterVec(
		"scheduler", "kafka_produce_failures_total",
		"Kafka produce failures on emails.due.",
		"pod_index",
	)
	mPodIndexGauge = metrics.NewGaugeVec(
		"scheduler", "pod_index",
		"Always 1, labelled with this pod's index and total pod count.",
		"pod_index", "total_pods",
	)
)

// RecordPodIdentity sets the identity gauge once at startup so dashboards can
// sanity-check sharding without parsing labels off other metrics.
func RecordPodIdentity(podIndex, totalPods int) {
	mPodIndexGauge.WithLabelValues(strconv.Itoa(podIndex), strconv.Itoa(totalPods)).Set(1)
}

// podLabel is the pod_index label value every per-pod metric carries.
func podLabel(podIndex int) string { return strconv.Itoa(podIndex) }
