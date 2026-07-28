package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// scrape renders the shared registry the way Prometheus sees it.
func scrape(t *testing.T) string {
	t.Helper()
	rr := httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	return rr.Body.String()
}

func TestLabelledMetricsAreNamespacedAndScrapable(t *testing.T) {
	NewCounterVec("probe", "events_total", "test events", "kind").WithLabelValues("ok").Inc()

	body := scrape(t)
	if !strings.Contains(body, "hatch_probe_events_total") {
		t.Fatalf("metric missing from /metrics output:\n%s", body)
	}
	if !strings.Contains(body, `kind="ok"`) {
		t.Errorf("label missing from /metrics output:\n%s", body)
	}
}

func TestUnlabelledMetricsNeedNoLabelValues(t *testing.T) {
	// The point of the plain constructors: no no-op WithLabelValues() at the
	// call site.
	NewCounter("probe", "plain_total", "unlabelled counter").Inc()
	NewGauge("probe", "plain_gauge", "unlabelled gauge").Set(3)
	NewHistogram("probe", "plain_seconds", "unlabelled histogram", []float64{1}).Observe(0.5)

	body := scrape(t)
	for _, want := range []string{"hatch_probe_plain_total 1", "hatch_probe_plain_gauge 3", "hatch_probe_plain_seconds_count 1"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}
