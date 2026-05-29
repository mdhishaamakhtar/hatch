package scheduler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mdhishaamakhtar/hatch/internal/scheduler"
	"go.uber.org/zap"
)

func makeID(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func newTestServer() (*scheduler.Server, *scheduler.Wheel) {
	w := scheduler.NewWheel()
	cfg := scheduler.Config{PodIndex: 0, TotalPods: 2, AdminAPIKey: "test-admin"}
	srv := scheduler.NewServer(cfg, zap.NewNop(), nil, w, func() bool { return true }, nil, nil)
	return srv, w
}

func TestAdminAuthRejectsMissingHeader(t *testing.T) {
	srv, _ := newTestServer()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/stats", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAdminAuthAcceptsCorrectKey(t *testing.T) {
	srv, _ := newTestServer()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/stats", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestStatsReportsWheelState(t *testing.T) {
	srv, w := newTestServer()
	w.Append(5, 10, makeID(1))
	w.Append(5, 10, makeID(2))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/stats", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
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

func TestPollTriggersAndReturns202(t *testing.T) {
	w := scheduler.NewWheel()
	cfg := scheduler.Config{PodIndex: 0, TotalPods: 2, AdminAPIKey: "test-admin"}
	trigger := make(chan struct{}, 1)
	srv := scheduler.NewServer(cfg, zap.NewNop(), nil, w, func() bool { return true }, nil, trigger)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/poll", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	select {
	case <-trigger:
	default:
		t.Fatal("expected a poll signal on the trigger channel")
	}
}

func TestPollWithoutTriggerStillReturns202(t *testing.T) {
	srv, _ := newTestServer() // nil trigger channel
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/poll", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}
}

func TestSlotReturnsUUIDStrings(t *testing.T) {
	srv, w := newTestServer()
	w.Append(3, 4, makeID(0xff))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/slots/03/04", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	var body struct {
		Slot        string   `json:"slot"`
		ScheduleIDs []string `json:"schedule_ids"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body.Slot != "03:04" {
		t.Errorf("slot = %q want 03:04", body.Slot)
	}
	if len(body.ScheduleIDs) != 1 || len(body.ScheduleIDs[0]) != 36 {
		t.Errorf("expected one 36-char UUID, got %v", body.ScheduleIDs)
	}
}

func TestSlotRejectsOutOfRange(t *testing.T) {
	srv, _ := newTestServer()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/wheel/slots/99/00", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}
