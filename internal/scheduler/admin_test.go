package scheduler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mdhishaamakhtar/hatch/internal/scheduler"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

const testAdminKey = "test-admin"

func makeID(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// newTestServer builds the admin surface over a pipeline with no live backing
// services — every /internal route reads the wheel and nothing else.
func newTestServer() (*scheduler.Server, *scheduler.Pipeline, *scheduler.Wheel) {
	cfg := scheduler.Config{PodIndex: 0, TotalPods: 2, AdminAPIKey: testAdminKey, ClearChannelBuffer: 1}
	wheel := scheduler.NewWheel()
	pipeline := scheduler.NewPipeline(cfg, zap.NewNop(), wheel, nil, nil, nil,
		noop.NewTracerProvider().Tracer("test"))
	return scheduler.NewServer(cfg, zap.NewNop(), nil, pipeline, func() bool { return true }), pipeline, wheel
}

// adminRequest issues an authenticated request against the admin surface.
func adminRequest(t *testing.T, srv *scheduler.Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer "+testAdminKey)
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func TestAdminAuthRejectsMissingHeader(t *testing.T) {
	srv, _, _ := newTestServer()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/internal/wheel/stats", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAdminAuthRejectsWrongKey(t *testing.T) {
	srv, _, _ := newTestServer()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/stats", nil)
	req.Header.Set("Authorization", "Bearer not-the-key")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestStatsReportsWheelState(t *testing.T) {
	srv, _, wheel := newTestServer()
	slot := scheduler.Slot{Min: 5, Sec: 10}
	wheel.Append(slot, makeID(1))
	wheel.Append(slot, makeID(2))

	rr := adminRequest(t, srv, http.MethodGet, "/internal/wheel/stats")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	var body struct {
		PodIndex      int `json:"pod_index"`
		TotalPods     int `json:"total_pods"`
		OccupiedSlots int `json:"occupied_slots"`
		TotalLoaded   int `json:"total_loaded"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.PodIndex != 0 || body.TotalPods != 2 || body.OccupiedSlots != 1 || body.TotalLoaded != 2 {
		t.Fatalf("unexpected stats: %+v", body)
	}
}

func TestPollSignalsTheTriggerChannel(t *testing.T) {
	srv, pipeline, _ := newTestServer()

	if rr := adminRequest(t, srv, http.MethodPost, "/internal/poll"); rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if !pipeline.PollPending() {
		t.Fatal("expected a poll signal queued on the trigger channel")
	}
}

// The trigger channel holds one pending signal, so a burst of poll requests
// coalesces into a single extra poll instead of blocking the handler.
func TestPollCoalescesWhenAlreadyPending(t *testing.T) {
	srv, _, _ := newTestServer()
	for range 3 {
		if rr := adminRequest(t, srv, http.MethodPost, "/internal/poll"); rr.Code != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", rr.Code)
		}
	}
}

func TestSlotReturnsUUIDStrings(t *testing.T) {
	srv, _, wheel := newTestServer()
	wheel.Append(scheduler.Slot{Min: 3, Sec: 4}, makeID(0xff))

	rr := adminRequest(t, srv, http.MethodGet, "/internal/wheel/slots/03/04")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	var body struct {
		Slot        string   `json:"slot"`
		ScheduleIDs []string `json:"schedule_ids"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Slot != "03:04" {
		t.Errorf("slot = %q, want 03:04", body.Slot)
	}
	want := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	if len(body.ScheduleIDs) != 1 || body.ScheduleIDs[0] != want {
		t.Errorf("schedule_ids = %v, want [%s]", body.ScheduleIDs, want)
	}
}

func TestSlotRejectsBadCoordinates(t *testing.T) {
	srv, _, _ := newTestServer()
	for _, path := range []string{
		"/internal/wheel/slots/99/00",
		"/internal/wheel/slots/00/99",
		"/internal/wheel/slots/xx/00",
		"/internal/wheel/slots/-1/00",
	} {
		if rr := adminRequest(t, srv, http.MethodGet, path); rr.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", path, rr.Code)
		}
	}
}

func TestHealthzIsUnauthenticated(t *testing.T) {
	srv, _, _ := newTestServer()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}
