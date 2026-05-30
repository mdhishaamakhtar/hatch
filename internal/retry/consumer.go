package retry

import (
	"context"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Kafka topics. emails.due is the re-enqueue target; the retry tiers are the
// inputs the delivery worker produces to on transient failure. Defined here
// (rather than imported from internal/delivery) to keep the package decoupled —
// the names are part of the cross-service contract in the HLD.
const (
	TopicEmailsDue  = "emails.due"
	TopicRetry1Min  = "emails.retry.1min"
	TopicRetry5Min  = "emails.retry.5min"
	TopicRetry30Min = "emails.retry.30min"
)

// Tier is one retry tier: its label, the topic it drains, the durable consumer
// group it owns, and how often it drains.
type Tier struct {
	Name     string
	Topic    string
	Group    string
	Interval time.Duration
}

// poller is the narrow franz-go consumer surface RunTier needs. *kgo.Client
// satisfies it.
type poller interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// RunTier drains t.Topic on t.Interval and re-enqueues every record to
// emails.due, until ctx is cancelled. It blocks; run one per goroutine.
func RunTier(ctx context.Context, t Tier, consumer poller, producer Producer, tr trace.Tracer, lg *zap.Logger, batchSize int, fetchMaxWait time.Duration) {
	lg = lg.With(zap.String("tier", t.Name))
	lg.Info("retry tier consumer started",
		zap.String("topic", t.Topic),
		zap.String("group", t.Group),
		zap.Duration("interval", t.Interval),
	)

	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drainOnce(ctx, t, consumer, producer, tr, lg, batchSize, fetchMaxWait)
		}
	}
}

// drainOnce drains every record currently on the tier topic and re-enqueues it
// to emails.due. It polls until a poll returns empty (the topic is momentarily
// drained), re-producing and committing one poll's worth at a time. A poll's
// offsets are committed only after all its records re-enqueue cleanly — on a
// produce error the batch is left uncommitted so it re-drains next cycle
// (at-least-once; duplicate emails.due records are deduped by the worker's Redis
// idempotency key).
func drainOnce(ctx context.Context, t Tier, consumer poller, producer Producer, tr trace.Tracer, lg *zap.Logger, batchSize int, fetchMaxWait time.Duration) {
	ctx, span := tr.Start(ctx, "retry.consumer.drain", trace.WithAttributes(attribute.String("tier", t.Name)))
	defer span.End()

	start := time.Now()
	drained := 0
	for {
		if ctx.Err() != nil {
			break
		}
		pollCtx, cancel := context.WithTimeout(ctx, fetchMaxWait)
		fetches := consumer.PollRecords(pollCtx, batchSize)
		cancel()
		if fetches.IsClientClosed() {
			break
		}
		// A fetch error here is the poll deadline firing on an empty topic; that
		// just means we've drained what's available, so stop the cycle.
		if len(fetches.Errors()) > 0 {
			break
		}

		recs := make([]*kgo.Record, 0, batchSize)
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			break
		}

		if !reEnqueue(ctx, t, recs, producer, tr, lg) {
			// Leave the batch uncommitted; next cycle re-drains it.
			break
		}
		if err := consumer.CommitRecords(ctx, recs...); err != nil {
			lg.Error("commit after re-enqueue failed", zap.Error(err), zap.Int("records", len(recs)))
			break
		}
		drained += len(recs)
	}

	span.SetAttributes(attribute.Int("drained", drained))
	mDrained.WithLabelValues(t.Name).Add(float64(drained))
	mDrainDuration.WithLabelValues(t.Name).Observe(time.Since(start).Seconds())
	if drained > 0 {
		lg.Info("drain cycle completed",
			zap.Int("drained", drained),
			zap.Duration("duration", time.Since(start)),
		)
	}
}

// reEnqueue re-produces each record to emails.due, carrying the original OTel
// trace context forward so the retry links back to the delivery attempt that
// failed. Returns false if any produce failed (caller skips the commit).
func reEnqueue(ctx context.Context, t Tier, recs []*kgo.Record, producer Producer, tr trace.Tracer, lg *zap.Logger) bool {
	ok := true
	for _, r := range recs {
		rctx := kafka.ExtractOtelHeaders(ctx, r)
		pctx, pspan := tr.Start(rctx, "kafka.produce.emails_due", trace.WithAttributes(attribute.String("tier", t.Name)))
		out := reEnqueueRecord(r)
		kafka.InjectOtelHeaders(pctx, out)
		err := producer.Produce(pctx, out)
		if err != nil {
			pspan.RecordError(err)
			mReenqueueFailures.WithLabelValues(t.Name).Inc()
			logger.WithCtx(pctx, lg).Error("re-enqueue to emails.due failed",
				zap.Error(err),
				zap.String("schedule_id", scheduleIDFromValue(r.Value)),
			)
			ok = false
		}
		pspan.End()
	}
	return ok
}

// reEnqueueRecord builds the emails.due record from a retry-tier record. The
// schedule_id payload and the partition key are preserved verbatim; only the
// topic changes (and the OTel headers are re-injected by the caller).
func reEnqueueRecord(r *kgo.Record) *kgo.Record {
	return &kgo.Record{
		Topic: TopicEmailsDue,
		Key:   append([]byte(nil), r.Key...),
		Value: append([]byte(nil), r.Value...),
	}
}
