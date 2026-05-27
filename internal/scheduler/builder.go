package scheduler

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// RunBuilder is G2: the sole writer to the wheel and to bbolt.
//
//   - in carries new (id, deliver_at) entries from G1.
//   - clear carries "MM:SS" slot keys from G3 after a slot has fired, so this
//     goroutine can delete the persisted state in lockstep.
//
// Running both as a single select keeps bbolt access serialised without an
// extra lock.
func RunBuilder(
	ctx context.Context,
	lg *zap.Logger,
	in <-chan Entry,
	clear <-chan string,
	w *Wheel,
	s WheelStore,
	podIndex int,
	tracer trace.Tracer,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-in:
			if !ok {
				return
			}
			handleAppend(ctx, lg, e, w, s, podIndex, tracer)
		case slot, ok := <-clear:
			if !ok {
				continue
			}
			if err := s.Delete(slot); err != nil {
				logger.WithCtx(ctx, lg).Error("bbolt delete failed",
					zap.String("slot", slot),
					zap.Error(err),
				)
			}
		}
	}
}

func handleAppend(
	ctx context.Context,
	lg *zap.Logger,
	e Entry,
	w *Wheel,
	s WheelStore,
	podIndex int,
	tracer trace.Tracer,
) {
	ctx, span := tracer.Start(ctx, "scheduler.wheel.load")
	defer span.End()

	mm := e.DeliverAt.Minute()
	ss := e.DeliverAt.Second()
	slot := SlotKey(mm, ss)
	span.SetAttributes(
		attribute.String("slot", slot),
		attribute.String("schedule_id", uuidString(e.ID)),
	)

	w.Append(mm, ss, e.ID)
	if err := s.Append(slot, e.ID); err != nil {
		span.RecordError(err)
		logger.WithCtx(ctx, lg).Error("bbolt append failed",
			zap.Int("pod_index", podIndex),
			zap.String("slot", slot),
			zap.String("schedule_id", uuidString(e.ID)),
			zap.Error(err),
		)
		return
	}

	// Keep gauges fresh — Stats() is O(3600) which is fine at insert cadence.
	occ, total := w.Stats()
	mWheelOccupied.With(podLabels(podIndex)).Set(float64(occ))
	mWheelTotalLoaded.With(podLabels(podIndex)).Set(float64(total))
}
