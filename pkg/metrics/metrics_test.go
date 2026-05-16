package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_servesRegistered(t *testing.T) {
	c := NewCounter("probe", "events_total", "test events", "kind")
	c.WithLabelValues("ok").Inc()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler().ServeHTTP(rr, req)

	body := rr.Body.String()
	if !strings.Contains(body, "hatch_probe_events_total") {
		t.Fatalf("metric not present in /metrics output:\n%s", body)
	}
	if !strings.Contains(body, `kind="ok"`) {
		t.Errorf("label not present: %s", body)
	}
}
