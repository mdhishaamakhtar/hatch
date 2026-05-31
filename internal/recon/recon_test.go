package recon

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// fakeProducer records the records it's asked to produce and can be made to fail.
type fakeProducer struct {
	got     []*kgo.Record
	failErr error
}

func (f *fakeProducer) Produce(_ context.Context, r *kgo.Record) error {
	f.got = append(f.got, r)
	return f.failErr
}

// fakeStore returns canned pass rows / errors.
type fakeStore struct {
	pass1 []gen.ReconPass1FreshAttemptRow
	pass2 []gen.ReconPass2OrphanedRetryRow
	err1  error
	err2  error
}

func (s fakeStore) ReconPass1FreshAttempt(context.Context) ([]gen.ReconPass1FreshAttemptRow, error) {
	return s.pass1, s.err1
}
func (s fakeStore) ReconPass2OrphanedRetry(context.Context) ([]gen.ReconPass2OrphanedRetryRow, error) {
	return s.pass2, s.err2
}

func testTracer() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample())))
}

func TestReconcileOnceProducesRecoveredIDs(t *testing.T) {
	testTracer()
	tr := otel.Tracer("test")

	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	store := fakeStore{
		pass1: []gen.ReconPass1FreshAttemptRow{{ID: db.UUIDToBytes(id1)}, {ID: db.UUIDToBytes(id2)}},
		pass2: []gen.ReconPass2OrphanedRetryRow{{ID: db.UUIDToBytes(id3)}},
	}
	fp := &fakeProducer{}

	p1, p2, err := ReconcileOnce(context.Background(), store, fp, tr, zap.NewNop())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if p1 != 2 || p2 != 1 {
		t.Fatalf("counts = (%d,%d), want (2,1)", p1, p2)
	}
	if len(fp.got) != 3 {
		t.Fatalf("produced %d records, want 3", len(fp.got))
	}

	wantIDs := map[string]bool{id1.String(): true, id2.String(): true, id3.String(): true}
	for _, r := range fp.got {
		if r.Topic != TopicEmailsDue {
			t.Errorf("topic = %q, want %q", r.Topic, TopicEmailsDue)
		}
		if len(r.Key) != 16 {
			t.Errorf("key len = %d, want 16-byte binary uuid", len(r.Key))
		}
		sid := scheduleIDOf(t, r.Value)
		if !wantIDs[sid] {
			t.Errorf("unexpected schedule_id %q", sid)
		}
		// The key bytes must round-trip to the same uuid as the payload.
		u, err := db.BytesToUUID(r.Key)
		if err != nil || u.String() != sid {
			t.Errorf("key uuid %v (%v) != payload schedule_id %q", u, err, sid)
		}
		if !hasHeader(r, "traceparent") {
			t.Errorf("missing traceparent header on re-enqueued record")
		}
	}
}

func TestReconcileOncePass1Error(t *testing.T) {
	testTracer()
	tr := otel.Tracer("test")
	store := fakeStore{err1: errors.New("db down")}
	fp := &fakeProducer{}
	if _, _, err := ReconcileOnce(context.Background(), store, fp, tr, zap.NewNop()); err == nil {
		t.Fatal("expected error when pass 1 fails")
	}
	if len(fp.got) != 0 {
		t.Fatalf("produced %d records on pass1 failure, want 0", len(fp.got))
	}
}

func TestDuePayload(t *testing.T) {
	got := string(duePayload("abc-123"))
	want := `{"schedule_id":"abc-123"}`
	if got != want {
		t.Errorf("duePayload = %s, want %s", got, want)
	}
}

func scheduleIDOf(t *testing.T, value []byte) string {
	t.Helper()
	var p struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(value, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return p.ScheduleID
}

func hasHeader(r *kgo.Record, key string) bool {
	for _, h := range r.Headers {
		if h.Key == key {
			return true
		}
	}
	return false
}
