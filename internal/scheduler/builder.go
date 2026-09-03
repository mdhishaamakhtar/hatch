package scheduler

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// RunBuilder is G2: the sole writer to the wheel and to bbolt.
//
// It serves two channels — new entries from G1, and slot keys from G3 for slots
// that have already fired. Handling both in one select serialises bbolt access
// without a second lock.
func (p *Pipeline) RunBuilder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-p.entries:
			if !ok {
				return
			}
			p.appendEntry(ctx, e)
		case slot := <-p.cleared:
			if err := p.store.Delete(slot); err != nil {
				logger.WithCtx(ctx, p.lg).Error("bbolt delete failed",
					zap.String("slot", slot),
					zap.Error(err),
				)
			}
		}
	}
}

// appendEntry places one schedule in its (mm, ss) slot, in memory and on disk.
func (p *Pipeline) appendEntry(ctx context.Context, e Entry) {
	ctx, span := p.tracer.Start(ctx, "scheduler.wheel.load")
	defer span.End()

	slot := SlotForDeliverAt(e.DeliverAt)
	span.SetAttributes(
		attribute.String("slot", slot.String()),
		attribute.String("schedule_id", uuidString(e.ID)),
	)

	// Recovery and the startup poll cover overlapping windows, so the same id
	// legitimately arrives twice. The wheel absorbs that in memory; persisting
	// again would leave a second copy in the slot's bbolt value.
	if !p.wheel.Append(slot, e.ID) {
		span.SetAttributes(attribute.Bool("duplicate", true))
		return
	}
	if err := p.store.Append(slot.String(), e.ID, e.DeliverAt); err != nil {
		span.RecordError(err)
		logger.WithCtx(ctx, p.lg).Error("bbolt append failed",
			zap.Int("pod_index", p.cfg.PodIndex),
			zap.String("slot", slot.String()),
			zap.String("schedule_id", uuidString(e.ID)),
			zap.Error(err),
		)
		return
	}
	p.publishWheelGauges()
}
