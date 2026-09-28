package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mdhishaamakhtar/hatch/internal/db"
)

const testAdminKey = "test-admin"

func adminRequest(s *Scheduler, method, path, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rr := httptest.NewRecorder()
	s.Handler(nil).ServeHTTP(rr, req)
	return rr
}

func TestInternalRoutesNeedTheAdminKey(t *testing.T) {
	s := newTestScheduler(t, &fakeLister{}, &fakeProducer{})
	for _, key := range []string{"", "wrong"} {
		if rr := adminRequest(s, http.MethodGet, "/internal/wheel/stats", key); rr.Code != http.StatusUnauthorized {
			t.Errorf("key %q: status %d, want 401", key, rr.Code)
		}
	}
}

func TestStatsReportsTheWheel(t *testing.T) {
	s := newTestScheduler(t, &fakeLister{}, &fakeProducer{})
	soon := time.Now().Add(time.Minute)
	if err := s.wheel.load([]db.ListDueRow{row(soon), row(soon)}, soon); err != nil {
		t.Fatal(err)
	}

	rr := adminRequest(s, http.MethodGet, "/internal/wheel/stats", testAdminKey)
	var got map[string]int
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil || rr.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rr.Code, rr.Body)
	}
	if got["total_loaded"] != 2 || got["pod_index"] != 0 || got["total_pods"] != 1 {
		t.Errorf("stats = %v", got)
	}
}

// Poll requests coalesce into one pending poll rather than blocking the handler.
func TestPollRequestsQueueOnePoll(t *testing.T) {
	s := newTestScheduler(t, &fakeLister{}, &fakeProducer{})
	for range 3 {
		if rr := adminRequest(s, http.MethodPost, "/internal/poll", testAdminKey); rr.Code != http.StatusAccepted {
			t.Fatalf("status %d, want 202", rr.Code)
		}
	}
	if len(s.pollNow) != 1 {
		t.Errorf("%d polls queued, want 1", len(s.pollNow))
	}
}
