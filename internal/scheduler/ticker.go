package scheduler

import (
	"context"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// RunTicker is G3. On each 1-second tick it drains the wheel slot matching the
// current (minute, second), produces one Kafka message per id to emails.due,
// and posts the slot key onto the cleared channel so G2 can delete the persisted
// state.
//
// tickC, if non-nil, replaces the internal ticker so tests can drive ticks
// explicitly.
func (p *Pipeline) RunTicker(ctx context.Context, tickC <-chan time.Time) {
	if tickC == nil {
		// Align to the top of the second before starting. A bare NewTicker
		// inherits whatever phase the pod happened to start on, so a slot could
		// fire most of a second into its own window — which, on top of the
		// wheel's one-second resolution, doubles the worst-case lateness. With
		// the alignment, a schedule fires within one second of its deliver_at.
		if !sleepUntilNextSecond(ctx, p.clock) {
			return
		}
		t := time.NewTicker(time.Second)
		defer t.Stop()
		tickC = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickC:
			p.fireTick(ctx, SlotOf(p.clock()))
		}
	}
}

// fireTick produces every id in one slot and hands the slot to G2 for cleanup.
func (p *Pipeline) fireTick(ctx context.Context, slot Slot) {
	ids := p.wheel.Drain(slot)
	if len(ids) == 0 {
		return
	}

	ctx, span := p.tracer.Start(ctx, "scheduler.wheel.fire")
	defer span.End()
	span.SetAttributes(
		attribute.String("slot", slot.String()),
		attribute.Int("schedule_ids_fired", len(ids)),
	)

	for _, id := range ids {
		p.produceOne(ctx, id)
	}

	// Non-blocking: the cleared channel has a generous buffer, and if it is
	// somehow full the next G2 turn catches the cleanup up anyway.
	select {
	case p.cleared <- slot.String():
	default:
		logger.WithCtx(ctx, p.lg).Warn("clear channel full; bbolt cleanup deferred",
			zap.String("slot", slot.String()),
		)
	}

	p.publishWheelGauges()
	logger.WithCtx(ctx, p.lg).Info("wheel slot fired",
		zap.Int("pod_index", p.cfg.PodIndex),
		zap.String("slot", slot.String()),
		zap.Int("count", len(ids)),
	)
}

func (p *Pipeline) produceOne(ctx context.Context, id [16]byte) {
	scheduleID := uuidString(id)
	ctx, span := p.tracer.Start(ctx, "kafka.produce.emails_due")
	defer span.End()
	span.SetAttributes(attribute.String("schedule_id", scheduleID))

	rec := kafka.NewDueRecord(kafka.TopicEmailsDue, id[:], scheduleID)
	kafka.InjectOtelHeaders(ctx, rec)

	label := podLabel(p.cfg.PodIndex)
	start := time.Now()
	err := p.producer.Produce(ctx, rec)
	mProduceDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
	if err != nil {
		mProduceFailures.WithLabelValues(label).Inc()
		span.RecordError(err)
		logger.WithCtx(ctx, p.lg).Error("kafka produce failure",
			zap.Int("pod_index", p.cfg.PodIndex),
			zap.String("schedule_id", scheduleID),
			zap.Error(err),
		)
	}
}

// sleepUntilNextSecond blocks until the next whole second, reporting false if
// ctx is cancelled first so a pod stopped during startup exits promptly.
func sleepUntilNextSecond(ctx context.Context, clock func() time.Time) bool {
	now := clock()
	t := time.NewTimer(now.Truncate(time.Second).Add(time.Second).Sub(now))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
