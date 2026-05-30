package delivery

import (
	"context"
	"errors"
	"time"

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

// clientGetter, idemAcquirer, and sendRouter are the narrow surfaces the
// processor needs from the cache, idempotency store, and router. *clientCache,
// *idempotency, and *Router satisfy them; tests use fakes.
type clientGetter interface {
	Get(ctx context.Context, clientID []byte) (clientInfo, error)
}

type idemAcquirer interface {
	Acquire(ctx context.Context, scheduleID string, retryCount int) (bool, error)
}

type sendRouter interface {
	Select(clientID string, providers []cachedProvider, lastProvider string) (vendor string, creds []byte, ok bool)
	Send(ctx context.Context, clientID, vendor string, creds []byte, e provider.Email) error
}

// Processor is G2: it drains one Batch at a time, runs each schedule through
// the delivery flow sequentially, then signals G1 to commit.
type Processor struct {
	lg         *zap.Logger
	store      Store
	cache      clientGetter
	idem       idemAcquirer
	router     sendRouter
	producer   Producer
	tracer     trace.Tracer
	maxRetries int
}

func NewProcessor(lg *zap.Logger, store Store, cache clientGetter, idem idemAcquirer, router sendRouter, producer Producer, tracer trace.Tracer, maxRetries int) *Processor {
	return &Processor{lg: lg, store: store, cache: cache, idem: idem, router: router, producer: producer, tracer: tracer, maxRetries: maxRetries}
}

// Compile-time checks that the concrete deps satisfy the processor's interfaces.
var (
	_ clientGetter = (*clientCache)(nil)
	_ idemAcquirer = (*idempotency)(nil)
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

	ids := make([][]byte, 0, len(b.recs))
	recByID := make(map[string]*kgo.Record, len(b.recs))
	for _, r := range b.recs {
		sid := scheduleIDFromValue(r.Value)
		if sid == "" {
			continue
		}
		u, err := parseUUID(sid)
		if err != nil {
			continue
		}
		ids = append(ids, db.UUIDToBytes(u))
		recByID[sid] = r
	}
	mBatchSize.WithLabelValues().Observe(float64(len(ids)))
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

	for i := range rows {
		row := rows[i]
		sid := uuidString(row.ID)
		rctx := ctx
		if rec := recByID[sid]; rec != nil {
			rctx = kafka.ExtractOtelHeaders(ctx, rec)
		}
		p.processOne(rctx, row)
	}
	mBatchDuration.WithLabelValues().Observe(time.Since(start).Seconds())
}

func (p *Processor) processOne(ctx context.Context, row gen.ScheduledEmail) {
	ctx, span := p.tracer.Start(ctx, "delivery.Batch.process")
	defer span.End()

	scheduleID := uuidString(row.ID)
	span.SetAttributes(attribute.String("schedule_id", scheduleID))
	lg := logger.WithCtx(ctx, p.lg).With(zap.String("schedule_id", scheduleID))

	// A row cancelled between produce and consume is skipped entirely.
	if row.Status == gen.ScheduleStatusCancelled {
		return
	}

	if err := p.store.MarkProcessing(ctx, gen.MarkProcessingParams{ID: row.ID, DeliverAt: row.DeliverAt}); err != nil {
		lg.Error("mark processing failed", zap.Error(err))
		return
	}

	info, err := p.cache.Get(ctx, row.ClientID)
	if err != nil {
		lg.Warn("client cache unavailable; leaving row processing", zap.Error(err))
		return
	}
	if !info.IsActive {
		p.markCancelled(ctx, lg, row, "client_inactive")
		mCancelled.WithLabelValues("client_inactive").Inc()
		return
	}

	acquired, err := p.idem.Acquire(ctx, scheduleID, int(row.RetryCount))
	if err != nil {
		lg.Warn("idempotency unavailable; leaving row processing", zap.Error(err))
		mIdem.WithLabelValues("unavailable").Inc()
		return
	}
	if !acquired {
		// Another worker already owns this send. Complete bookkeeping idempotently.
		mIdem.WithLabelValues("duplicate").Inc()
		p.markDelivered(ctx, lg, row, deref(row.LastProvider))
		return
	}
	mIdem.WithLabelValues("acquired").Inc()

	clientID := uuidString(row.ClientID)
	vendor, creds, ok := p.router.Select(clientID, info.Providers, deref(row.LastProvider))
	if !ok {
		p.markFailed(ctx, lg, row, "", "no_active_providers")
		mFailed.WithLabelValues("no_active_providers").Inc()
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

	sendCtx, sendSpan := p.tracer.Start(ctx, "provider.send")
	sendSpan.SetAttributes(attribute.String("provider", vendor), attribute.String("schedule_id", scheduleID))
	t0 := time.Now()
	sendErr := p.router.Send(sendCtx, clientID, vendor, creds, email)
	mSendDuration.WithLabelValues(vendor).Observe(time.Since(t0).Seconds())
	if sendErr != nil {
		sendSpan.RecordError(sendErr)
	}
	sendSpan.End()

	switch {
	case sendErr == nil:
		mSends.WithLabelValues(vendor, "success").Inc()
		p.markDelivered(ctx, lg, row, vendor)
		recordE2E(row.DeliverAt)
		lg.Info("email delivered", zap.String("provider", vendor))
	case errors.Is(sendErr, provider.ErrRateLimited) || errors.Is(sendErr, provider.ErrTransient):
		status := "transient"
		if errors.Is(sendErr, provider.ErrRateLimited) {
			status = "rate_limited"
		}
		mSends.WithLabelValues(vendor, status).Inc()
		p.handleRetry(ctx, lg, row, vendor, sendErr, scheduleID)
	default:
		// A non-transient, non-rate-limit error is permanent (e.g. bad credentials).
		mSends.WithLabelValues(vendor, "permanent_error").Inc()
		p.markFailed(ctx, lg, row, vendor, "provider_error: "+sendErr.Error())
		mFailed.WithLabelValues("provider_error").Inc()
	}
}

// handleRetry marks the row retrying and re-enqueues to the next tier, or fails
// it terminally once the retry budget is exhausted.
func (p *Processor) handleRetry(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor string, sendErr error, scheduleID string) {
	if int(row.RetryCount) >= p.maxRetries {
		p.markFailed(ctx, lg, row, vendor, "retry_exhausted: "+sendErr.Error())
		mFailed.WithLabelValues("retry_exhausted").Inc()
		return
	}
	next := int(row.RetryCount) + 1
	reason := sendErr.Error()
	if err := p.store.MarkRetrying(ctx, gen.MarkRetryingParams{
		ID:            row.ID,
		DeliverAt:     row.DeliverAt,
		LastProvider:  &vendor,
		FailureReason: &reason,
	}); err != nil {
		lg.Error("mark retrying failed", zap.Error(err))
		return
	}
	var id [16]byte
	copy(id[:], row.ID)
	if err := produceRetry(ctx, p.producer, id, scheduleID, next); err != nil {
		lg.Error("retry re-enqueue failed", zap.Error(err), zap.Int("tier", next))
		return
	}
	mRetries.WithLabelValues(retryTierLabel(next)).Inc()
	lg.Info("email retrying", zap.String("provider", vendor), zap.String("tier", retryTierLabel(next)))
}

func (p *Processor) markDelivered(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor string) {
	var lp *string
	if vendor != "" {
		lp = &vendor
	}
	if err := p.store.MarkDelivered(ctx, gen.MarkDeliveredParams{ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: lp}); err != nil {
		lg.Error("mark delivered failed", zap.Error(err))
	}
}

func (p *Processor) markFailed(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, vendor, reason string) {
	var lp *string
	if vendor != "" {
		lp = &vendor
	}
	if err := p.store.MarkFailed(ctx, gen.MarkFailedParams{ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: lp, FailureReason: &reason}); err != nil {
		lg.Error("mark failed failed", zap.Error(err))
	}
}

func (p *Processor) markCancelled(ctx context.Context, lg *zap.Logger, row gen.ScheduledEmail, reason string) {
	if err := p.store.MarkCancelled(ctx, gen.MarkCancelledParams{ID: row.ID, DeliverAt: row.DeliverAt, FailureReason: &reason}); err != nil {
		lg.Error("mark cancelled failed", zap.Error(err))
	}
}

func recordE2E(deliverAt pgtype.Timestamptz) {
	if deliverAt.Valid {
		mE2ELatency.WithLabelValues().Observe(time.Since(deliverAt.Time).Seconds())
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
