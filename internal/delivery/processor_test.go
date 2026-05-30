package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// --- fakes ---

type fakeStore struct {
	rows      []gen.ScheduledEmail
	processed int
	delivered []gen.MarkDeliveredParams
	retrying  []gen.MarkRetryingParams
	failed    []gen.MarkFailedParams
	cancelled []gen.MarkCancelledParams
}

func (f *fakeStore) BatchFetchSchedules(context.Context, [][]byte) ([]gen.ScheduledEmail, error) {
	return f.rows, nil
}
func (f *fakeStore) MarkProcessing(context.Context, gen.MarkProcessingParams) error {
	f.processed++
	return nil
}
func (f *fakeStore) MarkDelivered(_ context.Context, a gen.MarkDeliveredParams) error {
	f.delivered = append(f.delivered, a)
	return nil
}
func (f *fakeStore) MarkRetrying(_ context.Context, a gen.MarkRetryingParams) error {
	f.retrying = append(f.retrying, a)
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, a gen.MarkFailedParams) error {
	f.failed = append(f.failed, a)
	return nil
}
func (f *fakeStore) MarkCancelled(_ context.Context, a gen.MarkCancelledParams) error {
	f.cancelled = append(f.cancelled, a)
	return nil
}
func (f *fakeStore) GetClientForDelivery(context.Context, []byte) (bool, error) { return true, nil }
func (f *fakeStore) ListClientActiveProviders(context.Context, []byte) ([]gen.ListClientActiveProvidersRow, error) {
	return nil, nil
}

type fakeCache struct {
	info clientInfo
	err  error
}

func (f fakeCache) Get(context.Context, []byte) (clientInfo, error) { return f.info, f.err }

type fakeIdem struct {
	acquired bool
	err      error
}

func (f fakeIdem) Acquire(context.Context, string, int) (bool, error) { return f.acquired, f.err }

type fakeRouter struct {
	vendor  string
	ok      bool
	sendErr error
	sends   int
}

func (f *fakeRouter) Select(string, []cachedProvider, string) (string, []byte, bool) {
	return f.vendor, nil, f.ok
}
func (f *fakeRouter) Send(context.Context, string, string, []byte, provider.Email) error {
	f.sends++
	return f.sendErr
}

type fakeProducer struct{ recs []*kgo.Record }

func (f *fakeProducer) Produce(_ context.Context, r *kgo.Record) error {
	f.recs = append(f.recs, r)
	return nil
}

// --- helpers ---

func testRow(retry int16, status gen.ScheduleStatus) gen.ScheduledEmail {
	id := make([]byte, 16)
	id[0] = 0x11
	cid := make([]byte, 16)
	cid[0] = 0x22
	return gen.ScheduledEmail{
		ID:             id,
		ClientID:       cid,
		Status:         status,
		RetryCount:     retry,
		DeliverAt:      pgtype.Timestamptz{Time: time.Now(), Valid: true},
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "s",
		Body:           "<p>b</p>",
	}
}

func newTestProcessor(store *fakeStore, cache fakeCache, idem fakeIdem, router *fakeRouter, prod *fakeProducer) *Processor {
	return NewProcessor(zap.NewNop(), store, cache, idem, router, prod, noop.NewTracerProvider().Tracer("test"), 3)
}

func activeMock() clientInfo {
	return clientInfo{IsActive: true, Providers: provs("mock")}
}

// --- tests ---

func TestProcessOneDelivered(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true}
	prod := &fakeProducer{}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: true}, router, prod)

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(store.delivered) != 1 {
		t.Fatalf("want 1 delivered, got %d", len(store.delivered))
	}
	if got := deref(store.delivered[0].LastProvider); got != "mock" {
		t.Errorf("delivered last_provider = %q, want mock", got)
	}
	if store.processed != 1 {
		t.Errorf("want MarkProcessing called once, got %d", store.processed)
	}
}

func TestProcessOneTransientRetries(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true, sendErr: provider.ErrTransient}
	prod := &fakeProducer{}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: true}, router, prod)

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(store.retrying) != 1 {
		t.Fatalf("want 1 retrying, got %d", len(store.retrying))
	}
	if len(prod.recs) != 1 || prod.recs[0].Topic != TopicRetry1Min {
		t.Fatalf("want one re-enqueue to %s, got %+v", TopicRetry1Min, prod.recs)
	}
}

func TestProcessOneRetryExhausted(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true, sendErr: provider.ErrTransient}
	prod := &fakeProducer{}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: true}, router, prod)

	// retry_count already at the max (3) → terminal failure, no re-enqueue.
	p.processOne(context.Background(), testRow(3, gen.ScheduleStatusPending))

	if len(store.failed) != 1 {
		t.Fatalf("want 1 failed, got %d", len(store.failed))
	}
	if got := deref(store.failed[0].FailureReason); !hasPrefix(got, "retry_exhausted") {
		t.Errorf("failure_reason = %q, want retry_exhausted prefix", got)
	}
	if len(prod.recs) != 0 {
		t.Errorf("exhausted retries must not re-enqueue, got %d", len(prod.recs))
	}
}

func TestProcessOnePermanentFails(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "resend", ok: true, sendErr: errors.New("bad credentials")}
	p := newTestProcessor(store, fakeCache{info: clientInfo{IsActive: true, Providers: provs("resend")}}, fakeIdem{acquired: true}, router, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(store.failed) != 1 {
		t.Fatalf("want 1 failed, got %d", len(store.failed))
	}
	if len(store.retrying) != 0 {
		t.Error("permanent error must not retry")
	}
}

func TestProcessOneInactiveClientCancelled(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true}
	p := newTestProcessor(store, fakeCache{info: clientInfo{IsActive: false}}, fakeIdem{acquired: true}, router, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(store.cancelled) != 1 {
		t.Fatalf("want 1 cancelled, got %d", len(store.cancelled))
	}
	if got := deref(store.cancelled[0].FailureReason); got != "client_inactive" {
		t.Errorf("cancel reason = %q, want client_inactive", got)
	}
	if router.sends != 0 {
		t.Error("inactive client must not send")
	}
}

func TestProcessOneIdempotencyDuplicate(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: false}, router, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if router.sends != 0 {
		t.Error("duplicate must skip the provider send")
	}
	if len(store.delivered) != 1 {
		t.Fatalf("duplicate should still mark delivered, got %d", len(store.delivered))
	}
}

func TestProcessOneCacheUnavailableLeavesProcessing(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{vendor: "mock", ok: true}
	p := newTestProcessor(store, fakeCache{err: errCacheUnavailable}, fakeIdem{acquired: true}, router, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if store.processed != 1 {
		t.Errorf("row should be marked processing, got %d", store.processed)
	}
	if len(store.delivered)+len(store.failed)+len(store.cancelled)+len(store.retrying) != 0 {
		t.Error("cache-unavailable row must be left in processing (no terminal mark)")
	}
}

func TestProcessOneNoProviderFails(t *testing.T) {
	store := &fakeStore{}
	router := &fakeRouter{ok: false}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: true}, router, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(store.failed) != 1 {
		t.Fatalf("want 1 failed, got %d", len(store.failed))
	}
	if got := deref(store.failed[0].FailureReason); got != "no_active_providers" {
		t.Errorf("failure_reason = %q, want no_active_providers", got)
	}
}

func TestProcessOneSkipsCancelledRow(t *testing.T) {
	store := &fakeStore{}
	p := newTestProcessor(store, fakeCache{info: activeMock()}, fakeIdem{acquired: true}, &fakeRouter{ok: true}, &fakeProducer{})

	p.processOne(context.Background(), testRow(0, gen.ScheduleStatusCancelled))

	if store.processed != 0 {
		t.Error("an already-cancelled row must be skipped before MarkProcessing")
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
