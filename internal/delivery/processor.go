package delivery

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Batch carries one poll's worth of emails.due records from G1 to G2. The raw
// records are passed (not pre-parsed ids) so G2 can resume each message's OTel
// trace from its headers, and G1 can commit the exact offsets after the ack.
type Batch struct {
	recs []*kgo.Record
}

// clientGetter, idemStore, and sendRouter are the narrow surfaces the processor
// needs from the cache, idempotency store, and router. *clientCache,
// *idempotency, and *Router satisfy them; tests use fakes.
type clientGetter interface {
	Get(ctx context.Context, clientID []byte) (clientInfo, error)
}

type idemStore interface {
	Acquire(ctx context.Context, scheduleID string, retryCount int) (claimState, error)
	MarkSent(ctx context.Context, scheduleID string, retryCount int) error
}

type sendRouter interface {
	Select(clientID string, providers []cachedProvider, lastProvider string) Selection
	Send(ctx context.Context, clientID, vendor string, creds []byte, e provider.Email) error
}

// Processor is G2: it drains one Batch at a time, runs each schedule through
// the delivery flow sequentially, then signals G1 to commit.
type Processor struct {
	lg         *zap.Logger
	store      Store
	cache      clientGetter
	idem       idemStore
	router     sendRouter
	producer   kafka.Producer
	tracer     trace.Tracer
	maxRetries int
}

func NewProcessor(lg *zap.Logger, store Store, cache clientGetter, idem idemStore, router sendRouter, producer kafka.Producer, tracer trace.Tracer, maxRetries int) *Processor {
	return &Processor{lg: lg, store: store, cache: cache, idem: idem, router: router, producer: producer, tracer: tracer, maxRetries: maxRetries}
}

// Compile-time checks that the concrete deps satisfy the processor's interfaces.
var (
	_ clientGetter = (*clientCache)(nil)
	_ idemStore    = (*idempotency)(nil)
	_ sendRouter   = (*Router)(nil)
)

// Run is the G2 loop: receive a Batch, process it, signal ack, repeat.
func (p *Processor) Run(ctx context.Context, batchC <-chan Batch, ackC chan<- struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-batchC:
			p.processBatch(ctx, b)
			select {
			case ackC <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Processor) processBatch(ctx context.Context, b Batch) {
	start := time.Now()

	ids, recByID := parseBatch(b.recs)
	mBatchSize.Observe(float64(len(ids)))
	if len(ids) == 0 {
		return
	}

	rows, err := p.store.BatchFetchSchedules(ctx, ids)
	if err != nil {
		// Leave the rows as-is (pending); reconciliation re-enqueues. Don't ack-fail
		// the Batch — the records still get committed so we don't reprocess forever
		// on a poison Batch; Redis idempotency + reconciliation are the safety net.
		p.lg.Error("Batch fetch failed", zap.Error(err), zap.Int("ids", len(ids)))
		return
	}

	for _, row := range rows {
		// Stop between rows once shutdown starts. Continuing would only burn
		// failed status writes on a cancelled context; the uncommitted offsets
		// mean the next consumer picks these rows up cleanly.
		if ctx.Err() != nil {
			p.lg.Info("shutdown mid-batch; leaving remaining rows for redelivery")
			return
		}
		rowCtx := ctx
		if rec := recByID[uuidString(row.ID)]; rec != nil {
			rowCtx = kafka.ExtractOtelHeaders(ctx, rec)
		}
		p.processOne(rowCtx, row)
	}
	mBatchDuration.Observe(time.Since(start).Seconds())
}

// parseBatch extracts the binary schedule ids to fetch, plus a lookup from
// schedule id back to its record so each row can resume its own trace. Records
// with an unparseable payload are dropped — there is no row to act on.
func parseBatch(recs []*kgo.Record) (ids [][]byte, recByID map[string]*kgo.Record) {
	ids = make([][]byte, 0, len(recs))
	recByID = make(map[string]*kgo.Record, len(recs))
	for _, r := range recs {
		scheduleID := kafka.ScheduleIDFromValue(r.Value)
		u, err := uuid.Parse(scheduleID)
		if err != nil {
			continue
		}
		ids = append(ids, db.UUIDToBytes(u))
		recByID[scheduleID] = r
	}
	return ids, recByID
}

// isTerminal reports whether a row has reached a state it never leaves. A
// duplicate emails.due record for one of those must be dropped, not reprocessed.
func isTerminal(s gen.ScheduleStatus) bool {
	switch s {
	case gen.ScheduleStatusDelivered, gen.ScheduleStatusFailed, gen.ScheduleStatusCancelled:
		return true
	default:
		return false
	}
}

func (p *Processor) processOne(ctx context.Context, row gen.ScheduledEmail) {
	ctx, span := p.tracer.Start(ctx, "delivery.Batch.process")
	defer span.End()

	scheduleID := uuidString(row.ID)
	span.SetAttributes(attribute.String("schedule_id", scheduleID))
	lg := logger.WithCtx(ctx, p.lg).With(zap.String("schedule_id", scheduleID))

	// A row that reached a terminal state between produce and consume is done.
	// Reprocessing it would flip a delivered or failed row back to processing.
	if isTerminal(row.Status) {
		mSkipped.WithLabelValues(string(row.Status)).Inc()
		return
	}

	if !p.mark(ctx, lg, "processing", func() (int64, error) {
		return p.store.MarkProcessing(ctx, gen.MarkProcessingParams{ID: row.ID, DeliverAt: row.DeliverAt})
	}) {
		return
	}

	info, err := p.cache.Get(ctx, row.ClientID)
	if err != nil {
		lg.Warn("client cache unavailable; leaving row processing", zap.Error(err))
		return
	}
	if !info.IsActive {
		mCancelled.WithLabelValues("client_inactive").Inc()
		reason := "client_inactive"
		p.mark(ctx, lg, "cancelled", func() (int64, error) {
			return p.store.MarkCancelled(ctx, gen.MarkCancelledParams{
				ID: row.ID, DeliverAt: row.DeliverAt, FailureReason: &reason,
			})
		})
		return
	}

	retryCount := int(row.RetryCount)
	claim, err := p.idem.Acquire(ctx, scheduleID, retryCount)
	if err != nil {
		lg.Warn("idempotency unavailable; leaving row processing", zap.Error(err))
		mIdem.WithLabelValues("unavailable").Inc()
		return
	}
	switch claim {
	case claimSent:
		// Another worker sent this email and confirmed it. Only the bookkeeping
		// is left, and it is idempotent.
		mIdem.WithLabelValues("duplicate_sent").Inc()
		p.markDelivered(ctx, lg, row, deref(row.LastProvider))
		return
	case claimInFlight:
		// Another worker claimed the send but never confirmed it — it may still be
		// running, or it may have died mid-send. Marking delivered here would
		// record an email that possibly never went out, so do nothing: the row
		// stays `processing` and reconciliation (or the key's TTL) produces a
		// fresh, honest attempt.
		mIdem.WithLabelValues("duplicate_in_flight").Inc()
		lg.Info("send already claimed by another worker; leaving row processing")
		return
	}
	mIdem.WithLabelValues("acquired").Inc()

	clientID := uuidString(row.ClientID)
	sel := p.router.Select(clientID, info.Providers, deref(row.LastProvider))
	switch sel.Outcome {
	case SelectNoEligibleVendor:
		// A configuration problem, not a blip — no amount of retrying helps.
		mFailed.WithLabelValues("no_active_providers").Inc()
		p.markFailed(ctx, lg, row, "", "no_active_providers")
		return
	case SelectBreakerOpen, SelectNoCapacity:
		// Transient: an unhealthy vendor or an empty bucket. Both resolve on their
		// own, so hand the row to the retry tiers rather than failing it.
		reason := "provider_breaker_open"
		if sel.Outcome == SelectNoCapacity {
			reason = "provider_no_capacity"
		}
		mDeferred.WithLabelValues(reason).Inc()
		p.handleRetry(ctx, lg, row, "", errors.New(reason), scheduleID)
		return
	}

	email := provider.Email{
		ScheduleID:     row.ID,
		ClientID:       row.ClientID,
		RecipientEmail: row.RecipientEmail,
		FromEmail:      row.FromEmail,
		FromName:       deref(row.FromName),
		Subject:        row.Subject,
		Body:           row.Body,
	}
	sendErr := p.send(ctx, clientID, sel, email, scheduleID, retryCount)

	switch {
	case sendErr == nil:
		mSends.WithLabelValues(sel.Vendor, "success").Inc()
		p.markDelivered(ctx, lg, row, sel.Vendor)
		recordE2E(row.DeliverAt)
		lg.Info("email delivered", zap.String("provider", sel.Vendor))

	case errors.Is(sendErr, context.Canceled) || errors.Is(sendErr, context.DeadlineExceeded):
		// Cut short by our own shutdown, not by the provider. We don't know
		// whether the email went out, and the status write would fail on the same
		// dead context anyway. Leave the row `processing`; reconciliation owns it.
		mSends.WithLabelValues(sel.Vendor, "aborted").Inc()
		lg.Warn("send aborted by shutdown; leaving row processing", zap.Error(sendErr))

	case errors.Is(sendErr, provider.ErrRateLimited) || errors.Is(sendErr, provider.ErrTransient):
		status := "transient"
		if errors.Is(sendErr, provider.ErrRateLimited) {
			status = "rate_limited"
		}
		mSends.WithLabelValues(sel.Vendor, status).Inc()
		p.handleRetry(ctx, lg, row, sel.Vendor, sendErr, scheduleID)

	default:
		// A non-transient, non-rate-limit error is permanent (e.g. bad credentials).
		mSends.WithLabelValues(sel.Vendor, "permanent_error").Inc()
		mFailed.WithLabelValues("provider_error").Inc()
		p.markFailed(ctx, lg, row, sel.Vendor, "provider_error: "+sendErr.Error())
	}
}

// send performs the provider call under its own span and, on success, upgrades
// the idempotency claim to "sent" so a duplicate can safely complete the
// bookkeeping instead of having to guess.
func (p *Processor) send(ctx context.Context, clientID string, sel Selection, email provider.Email, scheduleID string, retryCount int) error {
	sendCtx, span := p.tracer.Start(ctx, "provider.send")
	defer span.End()
	span.SetAttributes(
		attribute.String("provider", sel.Vendor),
		attribute.String("schedule_id", scheduleID),
	)

	start := time.Now()
	err := p.router.Send(sendCtx, clientID, sel.Vendor, sel.Creds, email)
	mSendDuration.WithLabelValues(sel.Vendor).Observe(time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Best-effort: if this fails the key stays "claimed", so a later duplicate
	// re-sends rather than recording a delivery it cannot vouch for.
	if markErr := p.idem.MarkSent(ctx, scheduleID, retryCount); markErr != nil {
		logger.WithCtx(ctx, p.lg).Warn("idempotency mark-sent failed", zap.Error(markErr))
	}
	return nil
}

// handleRetry marks the row retrying and re-enqueues to the next tier, or fails
// it terminally once the retry budget is exhausted.
func (p *Processor) handleRetry(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor string, cause error, scheduleID string) {
	if int(row.RetryCount) >= p.maxRetries {
		mFailed.WithLabelValues("retry_exhausted").Inc()
		p.markFailed(ctx, lg, row, vendor, "retry_exhausted: "+cause.Error())
		return
	}
	next := int(row.RetryCount) + 1
	reason := cause.Error()
	if !p.mark(ctx, lg, "retrying", func() (int64, error) {
		return p.store.MarkRetrying(ctx, gen.MarkRetryingParams{
			ID:            row.ID,
			DeliverAt:     row.DeliverAt,
			LastProvider:  optional(vendor),
			FailureReason: &reason,
		})
	}) {
		return
	}
	if err := produceRetry(ctx, p.producer, row.ID, scheduleID, next); err != nil {
		lg.Error("retry re-enqueue failed", zap.Error(err), zap.Int("tier", next))
		return
	}
	tier, _ := tierFor(next)
	mRetries.WithLabelValues(tier).Inc()
	lg.Info("email retrying", zap.String("provider", vendor), zap.String("tier", tier))
}

func (p *Processor) markDelivered(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor string) {
	p.mark(ctx, lg, "delivered", func() (int64, error) {
		return p.store.MarkDelivered(ctx, gen.MarkDeliveredParams{
			ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: optional(vendor),
		})
	})
}

func (p *Processor) markFailed(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor, reason string) {
	p.mark(ctx, lg, "failed", func() (int64, error) {
		return p.store.MarkFailed(ctx, gen.MarkFailedParams{
			ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: optional(vendor), FailureReason: &reason,
		})
	})
}

// mark runs a guarded status write and reports whether the row actually moved.
// 0 rows means the status changed under us — a cancel that raced an in-flight
// send, or a duplicate record for an already-terminal row. The write is not
// retried and the caller stops: the row's current state is the true one.
func (p *Processor) mark(ctx context.Context, lg *zap.Logger, to string, write func() (int64, error)) bool {
	rows, err := write()
	if err != nil {
		lg.Error("status write failed", zap.String("to", to), zap.Error(err))
		return false
	}
	if rows == 0 {
		mLostRace.WithLabelValues(to).Inc()
		lg.Warn("status moved under this worker; leaving the row as it stands",
			zap.String("attempted", to))
		return false
	}
	return true
}

func recordE2E(deliverAt pgtype.Timestamptz) {
	if deliverAt.Valid {
		mE2ELatency.Observe(time.Since(deliverAt.Time).Seconds())
	}
}

// optional returns nil for the empty string so an unset column stays NULL.
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
