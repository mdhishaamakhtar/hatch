package delivery

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// --- fakes ---

// fakeStore records every status write. rowsAffected simulates a guarded UPDATE
// matching nothing, which is how a lost race presents itself.
type fakeStore struct {
	rows         []gen.ScheduledEmail
	fetchErr     error
	rowsAffected int64 // 0 in the zero value means "use 1"; see affected()
	loseRace     bool

	processed int
	delivered []gen.MarkDeliveredParams
	retrying  []gen.MarkRetryingParams
	failed    []gen.MarkFailedParams
	cancelled []gen.MarkCancelledParams
}

func (f *fakeStore) affected() (int64, error) {
	if f.loseRace {
		return 0, nil
	}
	return 1, nil
}

func (f *fakeStore) BatchFetchSchedules(context.Context, [][]byte) ([]gen.ScheduledEmail, error) {
	return f.rows, f.fetchErr
}
func (f *fakeStore) MarkProcessing(context.Context, gen.MarkProcessingParams) (int64, error) {
	f.processed++
	return f.affected()
}
func (f *fakeStore) MarkDelivered(_ context.Context, a gen.MarkDeliveredParams) (int64, error) {
	f.delivered = append(f.delivered, a)
	return f.affected()
}
func (f *fakeStore) MarkRetrying(_ context.Context, a gen.MarkRetryingParams) (int64, error) {
	f.retrying = append(f.retrying, a)
	return f.affected()
}
func (f *fakeStore) MarkFailed(_ context.Context, a gen.MarkFailedParams) (int64, error) {
	f.failed = append(f.failed, a)
	return f.affected()
}
func (f *fakeStore) MarkCancelled(_ context.Context, a gen.MarkCancelledParams) (int64, error) {
	f.cancelled = append(f.cancelled, a)
	return f.affected()
}
func (f *fakeStore) GetClientForDelivery(context.Context, []byte) (bool, error) { return true, nil }
func (f *fakeStore) ListClientActiveProviders(context.Context, []byte) ([]gen.ListClientActiveProvidersRow, error) {
	return nil, nil
}

// terminalWrites counts every status write that would end the row's life.
func (f *fakeStore) terminalWrites() int {
	return len(f.delivered) + len(f.failed) + len(f.cancelled) + len(f.retrying)
}

type fakeCache struct {
	info clientInfo
	err  error
}

func (f fakeCache) Get(context.Context, []byte) (clientInfo, error) { return f.info, f.err }

type fakeIdem struct {
	claim     claimState
	err       error
	markCalls int
	markErr   error
}

func (f *fakeIdem) Acquire(context.Context, string, int) (claimState, error) {
	return f.claim, f.err
}

func (f *fakeIdem) MarkSent(context.Context, string, int) error {
	f.markCalls++
	return f.markErr
}

type fakeRouter struct {
	selection Selection
	sendErr   error
	sends     int
}

func (f *fakeRouter) Select(string, []cachedProvider, string) Selection { return f.selection }
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

func testRow(retryCount int16, status gen.ScheduleStatus) gen.ScheduledEmail {
	scheduleID := make([]byte, 16)
	scheduleID[0] = 0x11
	clientID := make([]byte, 16)
	clientID[0] = 0x22
	return gen.ScheduledEmail{
		ID:             scheduleID,
		ClientID:       clientID,
		Status:         status,
		RetryCount:     retryCount,
		DeliverAt:      pgtype.Timestamptz{Time: time.Now(), Valid: true},
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "s",
		Body:           "<p>b</p>",
	}
}

// harness bundles the processor with the fakes the assertions read back.
type harness struct {
	proc   *Processor
	store  *fakeStore
	idem   *fakeIdem
	router *fakeRouter
	prod   *fakeProducer
}

// newHarness builds a processor whose happy path succeeds; each test overrides
// only the fake it cares about.
func newHarness(t *testing.T, opts ...func(*harness)) *harness {
	t.Helper()
	h := &harness{
		store:  &fakeStore{},
		idem:   &fakeIdem{claim: claimFree},
		router: &fakeRouter{selection: Selection{Outcome: SelectOK, Vendor: "mock"}},
		prod:   &fakeProducer{},
	}
	for _, opt := range opts {
		opt(h)
	}
	h.proc = NewProcessor(zap.NewNop(), h.store, fakeCache{info: activeClient("mock")},
		h.idem, h.router, h.prod, noop.NewTracerProvider().Tracer("test"), 3, 1)
	return h
}

// newHarnessWithCache is newHarness with a specific client-cache result.
func newHarnessWithCache(t *testing.T, cache fakeCache, opts ...func(*harness)) *harness {
	t.Helper()
	h := &harness{
		store:  &fakeStore{},
		idem:   &fakeIdem{claim: claimFree},
		router: &fakeRouter{selection: Selection{Outcome: SelectOK, Vendor: "mock"}},
		prod:   &fakeProducer{},
	}
	for _, opt := range opts {
		opt(h)
	}
	h.proc = NewProcessor(zap.NewNop(), h.store, cache, h.idem, h.router, h.prod,
		noop.NewTracerProvider().Tracer("test"), 3, 1)
	return h
}

func activeClient(vendors ...string) clientInfo {
	return clientInfo{IsActive: true, Providers: provs(vendors...)}
}

// --- happy path ---

func TestDeliveredMarksRowAndConfirmsTheClaim(t *testing.T) {
	h := newHarness(t)

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(h.store.delivered) != 1 {
		t.Fatalf("want 1 delivered write, got %d", len(h.store.delivered))
	}
	if got := deref(h.store.delivered[0].LastProvider); got != "mock" {
		t.Errorf("delivered last_provider = %q, want mock", got)
	}
	if h.store.processed != 1 {
		t.Errorf("MarkProcessing calls = %d, want 1", h.store.processed)
	}
	// The claim must be upgraded to "sent" after a successful send — that is what
	// lets a duplicate safely finish the bookkeeping instead of guessing.
	if h.idem.markCalls != 1 {
		t.Errorf("MarkSent calls = %d, want 1", h.idem.markCalls)
	}
}

func TestMarkSentFailureStillDelivers(t *testing.T) {
	// The email did go out; failing to record the claim upgrade must not change
	// the row's outcome.
	h := newHarness(t, func(h *harness) { h.idem.markErr = errors.New("redis down") })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(h.store.delivered) != 1 {
		t.Fatalf("want the row delivered anyway, got %d writes", len(h.store.delivered))
	}
}

// --- retry / failure classification ---

func TestTransientErrorRetriesOnTheNextTier(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.router.sendErr = provider.ErrTransient })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(h.store.retrying) != 1 {
		t.Fatalf("want 1 retrying write, got %d", len(h.store.retrying))
	}
	if len(h.prod.recs) != 1 || h.prod.recs[0].Topic != kafka.TopicRetry1Min {
		t.Fatalf("want one re-enqueue to %s, got %+v", kafka.TopicRetry1Min, h.prod.recs)
	}
}

func TestRetryExhaustionFailsWithoutReEnqueue(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.router.sendErr = provider.ErrTransient })

	// retry_count already at the configured max (3).
	h.proc.processOne(context.Background(), testRow(3, gen.ScheduleStatusPending))

	if len(h.store.failed) != 1 {
		t.Fatalf("want 1 failed write, got %d", len(h.store.failed))
	}
	if reason := deref(h.store.failed[0].FailureReason); !strings.HasPrefix(reason, "retry_exhausted") {
		t.Errorf("failure_reason = %q, want a retry_exhausted prefix", reason)
	}
	if len(h.prod.recs) != 0 {
		t.Errorf("exhausted retries must not re-enqueue, got %d records", len(h.prod.recs))
	}
}

func TestPermanentErrorFailsWithoutRetrying(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.router.sendErr = errors.New("bad credentials") })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(h.store.failed) != 1 {
		t.Fatalf("want 1 failed write, got %d", len(h.store.failed))
	}
	if len(h.store.retrying) != 0 {
		t.Error("a permanent error must not consume a retry")
	}
}

// A send cut short by our own shutdown says nothing about whether the email
// went out, so the row must be left alone for reconciliation rather than
// recorded as a provider failure.
func TestShutdownDuringSendLeavesRowUntouched(t *testing.T) {
	for _, sendErr := range []error{context.Canceled, context.DeadlineExceeded} {
		h := newHarness(t, func(h *harness) { h.router.sendErr = sendErr })

		h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

		if n := h.store.terminalWrites(); n != 0 {
			t.Errorf("%v: expected no terminal write, got %d", sendErr, n)
		}
		if h.store.processed != 1 {
			t.Errorf("%v: the row should still be marked processing", sendErr)
		}
	}
}

// --- routing outcomes ---

// Three very different conditions used to collapse into one "no providers"
// failure. Only a genuine configuration problem may be terminal.
func TestSelectOutcomesRouteToTheRightOutcome(t *testing.T) {
	cases := []struct {
		name       string
		outcome    SelectOutcome
		wantFailed bool
		wantRetry  bool
	}{
		{"no eligible vendor is a config problem", SelectNoEligibleVendor, true, false},
		{"an open breaker is a vendor outage", SelectBreakerOpen, false, true},
		{"an empty bucket is backpressure", SelectNoCapacity, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, func(h *harness) {
				h.router.selection = Selection{Outcome: c.outcome}
			})

			h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

			if got := len(h.store.failed) == 1; got != c.wantFailed {
				t.Errorf("failed=%v, want %v", got, c.wantFailed)
			}
			if got := len(h.store.retrying) == 1; got != c.wantRetry {
				t.Errorf("retrying=%v, want %v", got, c.wantRetry)
			}
			if h.router.sends != 0 {
				t.Error("no provider was selected, so nothing may be sent")
			}
		})
	}
}

func TestNoEligibleVendorRecordsTheReason(t *testing.T) {
	h := newHarness(t, func(h *harness) {
		h.router.selection = Selection{Outcome: SelectNoEligibleVendor}
	})

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if reason := deref(h.store.failed[0].FailureReason); reason != "no_active_providers" {
		t.Errorf("failure_reason = %q, want no_active_providers", reason)
	}
}

// --- idempotency claim states ---

// A claim that was never confirmed means the owner may have died before its
// send completed. Marking the row delivered here is how an unsent email gets
// recorded as delivered, so this path must do nothing at all.
func TestUnconfirmedClaimLeavesRowProcessing(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.idem.claim = claimInFlight })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if h.router.sends != 0 {
		t.Error("a claimed send must not be duplicated")
	}
	if n := h.store.terminalWrites(); n != 0 {
		t.Errorf("an unconfirmed claim must not write a terminal status, got %d writes", n)
	}
}

// A confirmed claim means the email definitely went out, so completing the
// bookkeeping is correct and idempotent.
func TestConfirmedClaimCompletesBookkeeping(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.idem.claim = claimSent })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if h.router.sends != 0 {
		t.Error("a confirmed send must not be repeated")
	}
	if len(h.store.delivered) != 1 {
		t.Fatalf("want the row marked delivered, got %d writes", len(h.store.delivered))
	}
}

func TestIdempotencyOutageLeavesRowProcessing(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.idem.err = errIdemUnavailable })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if h.router.sends != 0 {
		t.Error("must not send when the claim could not be checked")
	}
	if n := h.store.terminalWrites(); n != 0 {
		t.Errorf("expected no terminal write, got %d", n)
	}
}

// --- guards ---

func TestTerminalRowsAreSkippedEntirely(t *testing.T) {
	for _, status := range []gen.ScheduleStatus{
		gen.ScheduleStatusDelivered,
		gen.ScheduleStatusFailed,
		gen.ScheduleStatusCancelled,
	} {
		h := newHarness(t)

		h.proc.processOne(context.Background(), testRow(0, status))

		if h.store.processed != 0 {
			t.Errorf("%s: a terminal row must not be flipped back to processing", status)
		}
		if h.router.sends != 0 {
			t.Errorf("%s: a terminal row must not be sent", status)
		}
	}
}

// A guarded UPDATE matching nothing means the row moved under us — a cancel
// that raced the send, say. The processor must stop rather than press on and
// overwrite whatever the row now says.
func TestLostStatusRaceAbortsProcessing(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.store.loseRace = true })

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if h.router.sends != 0 {
		t.Error("losing the MarkProcessing race must stop before the send")
	}
	if n := h.store.terminalWrites(); n != 0 {
		t.Errorf("expected no terminal write after a lost race, got %d", n)
	}
}

func TestInactiveClientCancelsWithoutSending(t *testing.T) {
	h := newHarnessWithCache(t, fakeCache{info: clientInfo{IsActive: false}})

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if len(h.store.cancelled) != 1 {
		t.Fatalf("want 1 cancelled write, got %d", len(h.store.cancelled))
	}
	if reason := deref(h.store.cancelled[0].FailureReason); reason != "client_inactive" {
		t.Errorf("cancel reason = %q, want client_inactive", reason)
	}
	if h.router.sends != 0 {
		t.Error("an inactive client's mail must not be sent")
	}
}

func TestCacheOutageLeavesRowProcessing(t *testing.T) {
	h := newHarnessWithCache(t, fakeCache{err: errCacheUnavailable})

	h.proc.processOne(context.Background(), testRow(0, gen.ScheduleStatusPending))

	if h.store.processed != 1 {
		t.Errorf("the row should be marked processing, got %d calls", h.store.processed)
	}
	if n := h.store.terminalWrites(); n != 0 {
		t.Error("a cache-unavailable row must be left in processing")
	}
}

// --- batch handling ---

func TestParseBatchKeepsOnlyValidPayloads(t *testing.T) {
	valid := "0195e2c0-0000-7000-8000-000000000001"
	recs := []*kgo.Record{
		{Value: kafka.MarshalDuePayload(valid)},
		{Value: []byte(`not json`)},
		{Value: kafka.MarshalDuePayload("not-a-uuid")},
	}

	ids, byID := parseBatch(recs)

	if len(ids) != 1 || len(byID) != 1 {
		t.Fatalf("parseBatch kept %d ids / %d records, want 1 each", len(ids), len(byID))
	}
	if byID[valid] == nil {
		t.Errorf("the valid record should be reachable by its schedule id")
	}
}

func TestProcessBatchStopsOnFetchFailure(t *testing.T) {
	h := newHarness(t, func(h *harness) { h.store.fetchErr = errors.New("db down") })
	h.store.rows = []gen.ScheduledEmail{testRow(0, gen.ScheduleStatusPending)}

	h.proc.processBatch(context.Background(), Batch{recs: []*kgo.Record{
		{Value: kafka.MarshalDuePayload("0195e2c0-0000-7000-8000-000000000001")},
	}})

	if h.store.processed != 0 {
		t.Error("a failed fetch must not touch any row")
	}
}

// Once shutdown starts, remaining rows are left uncommitted for the next
// consumer rather than half-processed against a dying context.
func TestProcessBatchStopsAtShutdown(t *testing.T) {
	h := newHarness(t)
	h.store.rows = []gen.ScheduledEmail{
		testRow(0, gen.ScheduleStatusPending),
		testRow(0, gen.ScheduleStatusPending),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.proc.processBatch(ctx, Batch{recs: []*kgo.Record{
		{Value: kafka.MarshalDuePayload("0195e2c0-0000-7000-8000-000000000001")},
	}})

	if h.store.processed != 0 {
		t.Errorf("no row should be processed after cancellation, got %d", h.store.processed)
	}
}

// --- concurrency ---

// concurrentStore is a race-safe Store recording how many sends overlapped. The
// package's fakeStore is deliberately lock-free for the sequential tests, so the
// concurrency tests bring their own.
type concurrentStore struct {
	mu        sync.Mutex
	rows      []gen.ScheduledEmail
	delivered int
}

func (s *concurrentStore) BatchFetchSchedules(context.Context, [][]byte) ([]gen.ScheduledEmail, error) {
	return s.rows, nil
}
func (s *concurrentStore) MarkProcessing(context.Context, gen.MarkProcessingParams) (int64, error) {
	return 1, nil
}
func (s *concurrentStore) MarkDelivered(context.Context, gen.MarkDeliveredParams) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered++
	return 1, nil
}
func (s *concurrentStore) MarkRetrying(context.Context, gen.MarkRetryingParams) (int64, error) {
	return 1, nil
}
func (s *concurrentStore) MarkFailed(context.Context, gen.MarkFailedParams) (int64, error) {
	return 1, nil
}
func (s *concurrentStore) MarkCancelled(context.Context, gen.MarkCancelledParams) (int64, error) {
	return 1, nil
}
func (s *concurrentStore) GetClientForDelivery(context.Context, []byte) (bool, error) {
	return true, nil
}
func (s *concurrentStore) ListClientActiveProviders(context.Context, []byte) ([]gen.ListClientActiveProvidersRow, error) {
	return nil, nil
}

// blockingRouter holds each send for a fixed duration and tracks the high-water
// mark of concurrent sends, which is what the tests actually assert on.
type blockingRouter struct {
	hold time.Duration

	mu      sync.Mutex
	inWork  int
	maxSeen int
}

func (r *blockingRouter) Select(string, []cachedProvider, string) Selection {
	return Selection{Outcome: SelectOK, Vendor: "mock"}
}

func (r *blockingRouter) Send(context.Context, string, string, []byte, provider.Email) error {
	r.mu.Lock()
	r.inWork++
	if r.inWork > r.maxSeen {
		r.maxSeen = r.inWork
	}
	r.mu.Unlock()

	time.Sleep(r.hold)

	r.mu.Lock()
	r.inWork--
	r.mu.Unlock()
	return nil
}

func (r *blockingRouter) peak() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxSeen
}

// concurrentIdem is a race-safe idempotency store that always grants the claim.
type concurrentIdem struct{}

func (concurrentIdem) Acquire(context.Context, string, int) (claimState, error) {
	return claimFree, nil
}
func (concurrentIdem) MarkSent(context.Context, string, int) error { return nil }

func rowsForBench(n int) []gen.ScheduledEmail {
	out := make([]gen.ScheduledEmail, n)
	for i := range out {
		id := make([]byte, 16)
		id[0], id[1] = byte(i/256), byte(i%256)
		out[i] = gen.ScheduledEmail{
			ID:        id,
			ClientID:  make([]byte, 16),
			Status:    gen.ScheduleStatusPending,
			DeliverAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
		}
	}
	return out
}

func runConcurrentBatch(t *testing.T, rowCount, concurrency int, hold time.Duration) (*concurrentStore, *blockingRouter) {
	t.Helper()
	store := &concurrentStore{rows: rowsForBench(rowCount)}
	router := &blockingRouter{hold: hold}
	proc := NewProcessor(zap.NewNop(), store, fakeCache{info: activeClient("mock")},
		concurrentIdem{}, router, &fakeProducer{},
		noop.NewTracerProvider().Tracer("test"), 3, concurrency)

	recs := make([]*kgo.Record, 0, rowCount)
	for _, row := range store.rows {
		id, err := uuid.FromBytes(row.ID)
		if err != nil {
			t.Fatalf("uuid.FromBytes: %v", err)
		}
		recs = append(recs, kafka.NewDueRecord(kafka.TopicEmailsDue, row.ID, id.String()))
	}
	proc.processBatch(context.Background(), Batch{recs: recs})
	return store, router
}

// A send is almost entirely time spent waiting on the provider, so a batch must
// overlap them. Without concurrency a pod's throughput is pinned at
// 1/provider_latency no matter how many pods or Kafka partitions exist.
func TestProcessBatchSendsConcurrently(t *testing.T) {
	const rows, concurrency = 24, 8

	start := time.Now()
	store, router := runConcurrentBatch(t, rows, concurrency, 40*time.Millisecond)
	elapsed := time.Since(start)

	if store.delivered != rows {
		t.Errorf("delivered %d of %d rows", store.delivered, rows)
	}
	if peak := router.peak(); peak < 2 {
		t.Errorf("peak concurrent sends = %d; the batch was processed serially", peak)
	}
	// Serial would be 24*40ms = 960ms; at 8 at a time it is ~3 waves, ~120ms.
	if serial := rows * 40 * time.Millisecond; elapsed > serial/2 {
		t.Errorf("batch took %s, more than half the %s a serial batch would take", elapsed, serial)
	}
}

// The bound has to hold: it is what keeps a pod's Postgres pool, Redis client
// and provider from being handed an unbounded number of simultaneous sends.
func TestProcessBatchRespectsTheConcurrencyLimit(t *testing.T) {
	const rows, concurrency = 40, 4

	store, router := runConcurrentBatch(t, rows, concurrency, 20*time.Millisecond)

	if store.delivered != rows {
		t.Errorf("delivered %d of %d rows", store.delivered, rows)
	}
	if peak := router.peak(); peak > concurrency {
		t.Errorf("peak concurrent sends = %d, want at most %d", peak, concurrency)
	}
}

// concurrency=1 must still process every row, in the strictly serial shape the
// rest of the suite asserts against.
func TestProcessBatchSerialWhenConcurrencyIsOne(t *testing.T) {
	const rows = 6

	store, router := runConcurrentBatch(t, rows, 1, 5*time.Millisecond)

	if store.delivered != rows {
		t.Errorf("delivered %d of %d rows", store.delivered, rows)
	}
	if peak := router.peak(); peak != 1 {
		t.Errorf("peak concurrent sends = %d, want exactly 1", peak)
	}
}
