package delivery

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/provider"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// fakeStore records status writes. With loseRace set, every guarded write
// changes nothing, which is how a row that moved first looks.
type fakeStore struct {
	mu       sync.Mutex
	rows     []db.ScheduledEmail
	fetchErr error
	loseRace bool
	fetched  []db.GetSchedulesParams

	processing int
	delivered  []db.MarkDeliveredParams
	retrying   []db.MarkRetryingParams
	failed     []db.MarkFailedParams
	cancelled  []db.MarkCancelledParams
}

func (f *fakeStore) GetSchedules(_ context.Context, arg db.GetSchedulesParams) ([]db.ScheduledEmail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetched = append(f.fetched, arg)
	return f.rows, f.fetchErr
}

func (f *fakeStore) write(record func()) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record()
	if f.loseRace {
		return 0, nil
	}
	return 1, nil
}

func (f *fakeStore) MarkProcessing(context.Context, db.MarkProcessingParams) (int64, error) {
	return f.write(func() { f.processing++ })
}
func (f *fakeStore) MarkDelivered(_ context.Context, a db.MarkDeliveredParams) (int64, error) {
	return f.write(func() { f.delivered = append(f.delivered, a) })
}
func (f *fakeStore) MarkRetrying(_ context.Context, a db.MarkRetryingParams) (int64, error) {
	return f.write(func() { f.retrying = append(f.retrying, a) })
}
func (f *fakeStore) MarkFailed(_ context.Context, a db.MarkFailedParams) (int64, error) {
	return f.write(func() { f.failed = append(f.failed, a) })
}
func (f *fakeStore) MarkCancelled(_ context.Context, a db.MarkCancelledParams) (int64, error) {
	return f.write(func() { f.cancelled = append(f.cancelled, a) })
}

// finished counts every write that ends or parks the row.
func (f *fakeStore) finished() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered) + len(f.retrying) + len(f.failed) + len(f.cancelled)
}

type fakeClients struct {
	client client
	err    error
}

func (f fakeClients) lookup(context.Context, uuid.UUID) (client, error) { return f.client, f.err }

type fakeClaims struct {
	mu         sync.Mutex
	state      claimState
	err        error
	confirmErr error
	confirmed  int
}

func (f *fakeClaims) claim(context.Context, uuid.UUID, int16) (claimState, error) {
	return f.state, f.err
}

func (f *fakeClaims) confirmSent(context.Context, uuid.UUID, int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed++
	return f.confirmErr
}

type fakeProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rs...)
	var out kgo.ProduceResults
	for _, r := range rs {
		out = append(out, kgo.ProduceResult{Record: r})
	}
	return out
}

// stubProvider answers every send with err after hold, and tracks how many
// sends overlapped.
type stubProvider struct {
	err  error
	hold time.Duration

	mu             sync.Mutex
	sent, inFlight int
	peak           int
}

func (s *stubProvider) Send(ctx context.Context, _ provider.Email) error {
	s.mu.Lock()
	s.sent++
	s.inFlight++
	s.peak = max(s.peak, s.inFlight)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	if s.hold > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.hold):
		}
	}
	return s.err
}

func (s *stubProvider) sends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

type harness struct {
	w        *Worker
	store    *fakeStore
	claims   *fakeClaims
	prov     *stubProvider
	producer *fakeProducer
}

// newHarness builds a worker whose every step succeeds; tests break the one
// they are about.
func newHarness() *harness {
	h := &harness{
		store:    &fakeStore{},
		claims:   &fakeClaims{state: claimAcquired},
		prov:     &stubProvider{},
		producer: &fakeProducer{},
	}
	h.w = &Worker{
		lg:          zap.NewNop(),
		producer:    h.producer,
		store:       h.store,
		clients:     fakeClients{client: client{Active: true, Providers: []cachedProvider{{Vendor: "mock"}}}},
		claims:      h.claims,
		router:      testRouter(h.prov, 1000),
		concurrency: 1,
	}
	return h
}

func testRow(retryCount int16, status db.ScheduleStatus) db.ScheduledEmail {
	deliverAt := time.Now().Truncate(time.Millisecond)
	id := db.NewScheduleID(deliverAt)
	clientID := uuid.New()
	return db.ScheduledEmail{
		ID:             id[:],
		ClientID:       clientID[:],
		DeliverAt:      deliverAt,
		Status:         status,
		RetryCount:     retryCount,
		RecipientEmail: "to@example.com",
		FromEmail:      "from@example.com",
		Subject:        "s",
		Body:           "<p>b</p>",
	}
}

func (h *harness) process(row db.ScheduledEmail) { h.w.processOne(context.Background(), row) }

func TestSuccessfulSendIsDeliveredAndConfirmed(t *testing.T) {
	h := newHarness()
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.delivered) != 1 || value(h.store.delivered[0].LastProvider) != "mock" {
		t.Fatalf("delivered writes = %+v, want one via mock", h.store.delivered)
	}
	// Confirming the claim is what lets a duplicate record finish the
	// bookkeeping instead of guessing.
	if h.claims.confirmed != 1 {
		t.Errorf("confirmSent called %d times, want 1", h.claims.confirmed)
	}
}

func TestFailingToConfirmStillDelivers(t *testing.T) {
	h := newHarness()
	h.claims.confirmErr = errors.New("redis down")
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.delivered) != 1 {
		t.Fatalf("delivered %d rows, want the sent email recorded anyway", len(h.store.delivered))
	}
}

func TestTransientFailuresClimbTheRetryTiers(t *testing.T) {
	for attempt, tier := range kafka.RetryTiers {
		h := newHarness()
		h.prov.err = provider.ErrTransient
		h.process(testRow(int16(attempt), db.ScheduleStatusPending))

		if len(h.store.retrying) != 1 {
			t.Fatalf("attempt %d: %d retrying writes, want 1", attempt, len(h.store.retrying))
		}
		if len(h.producer.records) != 1 || h.producer.records[0].Topic != tier.Topic {
			t.Errorf("attempt %d: parked on %v, want %s", attempt, h.producer.records, tier.Topic)
		}
	}
}

func TestRetriesRunOutAfterTheLastTier(t *testing.T) {
	h := newHarness()
	h.prov.err = provider.ErrRateLimited
	h.process(testRow(int16(len(kafka.RetryTiers)), db.ScheduleStatusPending))

	if len(h.store.failed) != 1 || !strings.HasPrefix(value(h.store.failed[0].FailureReason), "retry_exhausted") {
		t.Fatalf("failed writes = %+v, want one retry_exhausted", h.store.failed)
	}
	if len(h.producer.records) != 0 {
		t.Error("an exhausted schedule was parked on a retry tier")
	}
}

func TestPermanentErrorFailsWithoutRetrying(t *testing.T) {
	h := newHarness()
	h.prov.err = errors.New("invalid credentials")
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.failed) != 1 || len(h.store.retrying) != 0 {
		t.Fatalf("failed=%d retrying=%d, want a failure and no retry", len(h.store.failed), len(h.store.retrying))
	}
}

// A send cut off by our own shutdown says nothing about whether the email went
// out, so the row is left for reconciliation instead of being judged.
func TestSendCutOffByShutdownLeavesTheRowProcessing(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		h := newHarness()
		h.prov.err = err
		h.process(testRow(0, db.ScheduleStatusPending))

		if h.store.processing != 1 || h.store.finished() != 0 {
			t.Errorf("%v: processing=%d finished=%d, want the row left processing", err, h.store.processing, h.store.finished())
		}
	}
}

func TestNoProvidersIsATerminalFailure(t *testing.T) {
	h := newHarness()
	h.w.clients = fakeClients{client: client{Active: true}}
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.failed) != 1 || value(h.store.failed[0].FailureReason) != "no_active_providers" {
		t.Fatalf("failed writes = %+v, want no_active_providers", h.store.failed)
	}
}

// Running out of rate-limit headroom is backpressure, not a failure.
func TestNoCapacityDefersToARetryTier(t *testing.T) {
	h := newHarness()
	h.w.router = testRouter(h.prov, 0)
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.retrying) != 1 || h.prov.sends() != 0 {
		t.Fatalf("retrying=%d sends=%d, want a deferral without a send", len(h.store.retrying), h.prov.sends())
	}
}

// A claim nobody confirmed means its owner may have died mid-send. Recording a
// delivery then could record an email that never went out.
func TestUnconfirmedClaimLeavesTheRowAlone(t *testing.T) {
	h := newHarness()
	h.claims.state = claimInFlight
	h.process(testRow(0, db.ScheduleStatusPending))

	if h.prov.sends() != 0 || h.store.finished() != 0 {
		t.Fatalf("sends=%d finished=%d, want nothing done", h.prov.sends(), h.store.finished())
	}
}

func TestConfirmedClaimOnlyFinishesTheBookkeeping(t *testing.T) {
	h := newHarness()
	h.claims.state = claimSent
	h.process(testRow(0, db.ScheduleStatusPending))

	if h.prov.sends() != 0 || len(h.store.delivered) != 1 {
		t.Fatalf("sends=%d delivered=%d, want the row delivered without resending", h.prov.sends(), len(h.store.delivered))
	}
}

func TestOutagesLeaveTheRowProcessing(t *testing.T) {
	claimsDown := newHarness()
	claimsDown.claims.err = errors.New("redis down")
	clientsDown := newHarness()
	clientsDown.w.clients = fakeClients{err: errors.New("redis down")}

	for name, h := range map[string]*harness{"claims": claimsDown, "client cache": clientsDown} {
		h.process(testRow(0, db.ScheduleStatusPending))
		if h.store.processing != 1 || h.prov.sends() != 0 || h.store.finished() != 0 {
			t.Errorf("%s outage: processing=%d sends=%d finished=%d", name, h.store.processing, h.prov.sends(), h.store.finished())
		}
	}
}

func TestFinishedSchedulesAreSkipped(t *testing.T) {
	for _, status := range []db.ScheduleStatus{db.ScheduleStatusDelivered, db.ScheduleStatusFailed, db.ScheduleStatusCancelled} {
		h := newHarness()
		h.process(testRow(0, status))
		if h.store.processing != 0 || h.prov.sends() != 0 {
			t.Errorf("%s: a finished schedule was reprocessed", status)
		}
	}
}

// A guarded write changing nothing means the row moved first (a cancel racing
// the send, say); the worker must stop rather than overwrite it.
func TestLosingTheStatusRaceStops(t *testing.T) {
	h := newHarness()
	h.store.loseRace = true
	h.process(testRow(0, db.ScheduleStatusPending))

	if h.prov.sends() != 0 {
		t.Fatal("sent after MarkProcessing changed nothing")
	}
}

func TestDeactivatedClientsAreCancelled(t *testing.T) {
	h := newHarness()
	h.w.clients = fakeClients{client: client{Active: false}}
	h.process(testRow(0, db.ScheduleStatusPending))

	if len(h.store.cancelled) != 1 || value(h.store.cancelled[0].FailureReason) != "client_inactive" || h.prov.sends() != 0 {
		t.Fatalf("cancelled=%+v sends=%d, want a cancel and no send", h.store.cancelled, h.prov.sends())
	}
}

func dueRecords(rows []db.ScheduledEmail) []*kgo.Record {
	records := make([]*kgo.Record, len(rows))
	for i, row := range rows {
		records[i] = kafka.DueRecord(context.Background(), kafka.TopicDue, uuid.UUID(row.ID))
	}
	return records
}

// The batch is fetched with each schedule's deliver_at, which is what lets
// Postgres touch only the partitions involved.
func TestBatchIsFetchedWithDeliverAts(t *testing.T) {
	h := newHarness()
	row := testRow(0, db.ScheduleStatusPending)
	bad := &kgo.Record{Value: []byte("not a uuid")}

	h.w.processBatch(context.Background(), append(dueRecords([]db.ScheduledEmail{row}), bad))

	if len(h.store.fetched) != 1 {
		t.Fatalf("fetched %d times, want once", len(h.store.fetched))
	}
	got := h.store.fetched[0]
	if len(got.Ids) != 1 || len(got.DeliverAts) != 1 || !got.DeliverAts[0].Equal(row.DeliverAt) {
		t.Fatalf("fetched %+v, want the one readable id with its deliver_at %v", got, row.DeliverAt)
	}
}

func TestBatchStopsStartingSendsAtShutdown(t *testing.T) {
	h := newHarness()
	h.store.rows = []db.ScheduledEmail{testRow(0, db.ScheduleStatusPending), testRow(0, db.ScheduleStatusPending)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h.w.processBatch(ctx, dueRecords(h.store.rows))

	if h.store.processing != 0 {
		t.Errorf("processed %d rows after shutdown, want 0", h.store.processing)
	}
}

// Sends are nearly all waiting on the provider, so a batch has to overlap
// them, up to the configured concurrency and never beyond it. The bubble's
// fake clock makes the timing exact: rows/concurrency rounds of one hold each.
func TestBatchSendsConcurrentlyUpToTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rows, concurrency = 24, 4
		h := newHarness()
		h.prov.hold = time.Second
		h.w.concurrency = concurrency
		for range rows {
			h.store.rows = append(h.store.rows, testRow(0, db.ScheduleStatusPending))
		}

		start := time.Now()
		h.w.processBatch(t.Context(), dueRecords(h.store.rows))
		elapsed := time.Since(start)

		if len(h.store.delivered) != rows {
			t.Errorf("delivered %d of %d", len(h.store.delivered), rows)
		}
		if h.prov.peak != concurrency {
			t.Errorf("peak concurrent sends = %d, want %d", h.prov.peak, concurrency)
		}
		if want := rows / concurrency * h.prov.hold; elapsed != want {
			t.Errorf("batch took %s, want %s (a serial batch would take %s)", elapsed, want, rows*h.prov.hold)
		}
	})
}
