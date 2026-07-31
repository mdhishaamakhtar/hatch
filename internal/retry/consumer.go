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

// Tier is one retry tier: its label, the topic it drains, the durable consumer
// group it owns, and how often it drains.
type Tier struct {
	Name     string
	Topic    string
	Group    string
	Interval time.Duration
}

// poller is the narrow franz-go consumer surface a drainer needs. *kgo.Client
// satisfies it; tests use a fake.
type poller interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// Drainer owns one tier: it drains that tier's topic on the tier interval and
// re-enqueues every record to emails.due. One Drainer runs per goroutine.
type Drainer struct {
	tier         Tier
	consumer     poller
	producer     kafka.Producer
	tracer       trace.Tracer
	lg           *zap.Logger
	batchSize    int
	fetchMaxWait time.Duration
}

// NewDrainer wires a tier's consumer to the shared producer. lg is tagged with
// the tier name so every line from this drainer is attributable.
func NewDrainer(t Tier, consumer poller, producer kafka.Producer, tr trace.Tracer, lg *zap.Logger, cfg Config) *Drainer {
	return &Drainer{
		tier:         t,
		consumer:     consumer,
		producer:     producer,
		tracer:       tr,
		lg:           lg.With(zap.String("tier", t.Name)),
		batchSize:    cfg.DrainBatchSize,
		fetchMaxWait: cfg.FetchMaxWait,
	}
}

// Run drains the tier on its interval until ctx is cancelled. It blocks.
func (d *Drainer) Run(ctx context.Context) {
	d.lg.Info("retry tier consumer started",
		zap.String("topic", d.tier.Topic),
		zap.String("group", d.tier.Group),
		zap.Duration("interval", d.tier.Interval),
	)

	t := time.NewTicker(d.tier.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.drainOnce(ctx)
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
func (d *Drainer) drainOnce(ctx context.Context) {
	ctx, span := d.tracer.Start(ctx, "retry.consumer.drain",
		trace.WithAttributes(attribute.String("tier", d.tier.Name)))
	defer span.End()

	start := time.Now()
	drained := 0
	for ctx.Err() == nil {
		recs, ok := d.poll(ctx)
		if !ok || len(recs) == 0 {
			break
		}
		if !d.reEnqueue(ctx, recs) {
			break // leave the batch uncommitted; next cycle re-drains it
		}
		if err := d.consumer.CommitRecords(ctx, recs...); err != nil {
			d.lg.Error("commit after re-enqueue failed", zap.Error(err), zap.Int("records", len(recs)))
			break
		}
		drained += len(recs)
	}

	span.SetAttributes(attribute.Int("drained", drained))
	mDrained.WithLabelValues(d.tier.Name).Add(float64(drained))
	mDrainDuration.WithLabelValues(d.tier.Name).Observe(time.Since(start).Seconds())
	if drained > 0 {
		d.lg.Info("drain cycle completed",
			zap.Int("drained", drained),
			zap.Duration("duration", time.Since(start)),
		)
	}
}

// poll fetches one batch. ok=false means the cycle should stop: either the
// client closed, or the poll deadline fired on an empty topic (which is how a
// fully-drained tier presents itself).
func (d *Drainer) poll(ctx context.Context) (recs []*kgo.Record, ok bool) {
	pollCtx, cancel := context.WithTimeout(ctx, d.fetchMaxWait)
	defer cancel()

	fetches := d.consumer.PollRecords(pollCtx, d.batchSize)
	if fetches.IsClientClosed() || len(fetches.Errors()) > 0 {
		return nil, false
	}
	recs = make([]*kgo.Record, 0, d.batchSize)
	fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	return recs, true
}

// reEnqueue re-produces each record to emails.due, carrying the original OTel
// trace context forward so the retry links back to the delivery attempt that
// failed. Returns false if any produce failed (caller skips the commit).
func (d *Drainer) reEnqueue(ctx context.Context, recs []*kgo.Record) bool {
	ok := true
	for _, r := range recs {
		rctx := kafka.ExtractOtelHeaders(ctx, r)
		pctx, pspan := d.tracer.Start(rctx, "kafka.produce.emails_due",
			trace.WithAttributes(attribute.String("tier", d.tier.Name)))
		out := reEnqueueRecord(r)
		kafka.InjectOtelHeaders(pctx, out)
		if err := d.producer.Produce(pctx, out); err != nil {
			pspan.RecordError(err)
			mReenqueueFailures.WithLabelValues(d.tier.Name).Inc()
			logger.WithCtx(pctx, d.lg).Error("re-enqueue to emails.due failed",
				zap.Error(err),
				zap.String("schedule_id", kafka.ScheduleIDFromValue(r.Value)),
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
		Topic: kafka.TopicEmailsDue,
		Key:   append([]byte(nil), r.Key...),
		Value: append([]byte(nil), r.Value...),
	}
}
