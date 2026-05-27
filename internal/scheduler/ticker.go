package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// TopicEmailsDue is the Kafka topic the scheduler produces to. Phase 3
// (delivery worker) consumes the same topic.
const TopicEmailsDue = "emails.due"

// emailsDuePayload is the JSON shape produced to TopicEmailsDue.
type emailsDuePayload struct {
	ScheduleID string `json:"schedule_id"`
}

// RunTicker is G3. On each 1-second tick it drains the wheel slot matching the
// current (minute, second), produces one Kafka message per id to emails.due,
// and posts the slot key onto clear so G2 can delete the persisted state.
//
// tickC, if non-nil, replaces the internal ticker — tests drive ticks
// explicitly via a channel.
func RunTicker(
	ctx context.Context,
	lg *zap.Logger,
	w *Wheel,
	clear chan<- string,
	producer MessageProducer,
	podIndex int,
	tracer trace.Tracer,
	tickC <-chan time.Time,
	now func() time.Time,
) {
	if tickC == nil {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		tickC = t.C
	}
	if now == nil {
		now = time.Now
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickC:
			fireTick(ctx, lg, w, clear, producer, podIndex, tracer, now())
		}
	}
}

func fireTick(
	ctx context.Context,
	lg *zap.Logger,
	w *Wheel,
	clear chan<- string,
	producer MessageProducer,
	podIndex int,
	tracer trace.Tracer,
	t time.Time,
) {
	mm, ss := t.Minute(), t.Second()
	ids := w.Drain(mm, ss)
	if len(ids) == 0 {
		return
	}

	slot := SlotKey(mm, ss)
	spanCtx, span := tracer.Start(ctx, "scheduler.wheel.fire")
	defer span.End()
	span.SetAttributes(
		attribute.String("slot", slot),
		attribute.Int("schedule_ids_fired", len(ids)),
	)

	for _, id := range ids {
		produceOne(spanCtx, lg, producer, podIndex, id, tracer)
	}

	// Non-blocking send. clear has a generous buffer; if it's somehow full,
	// log and continue — bbolt cleanup will catch up on the next G2 turn.
	select {
	case clear <- slot:
	default:
		logger.WithCtx(spanCtx, lg).Warn("clear channel full; bbolt cleanup deferred",
			zap.String("slot", slot),
		)
	}

	occ, total := w.Stats()
	mWheelOccupied.With(podLabels(podIndex)).Set(float64(occ))
	mWheelTotalLoaded.With(podLabels(podIndex)).Set(float64(total))

	logger.WithCtx(spanCtx, lg).Info("wheel slot fired",
		zap.Int("pod_index", podIndex),
		zap.String("slot", slot),
		zap.Int("count", len(ids)),
	)
}

func produceOne(
	ctx context.Context,
	lg *zap.Logger,
	producer MessageProducer,
	podIndex int,
	id [16]byte,
	tracer trace.Tracer,
) {
	scheduleID := uuidString(id)
	ctx, span := tracer.Start(ctx, "kafka.produce.emails_due")
	defer span.End()
	span.SetAttributes(attribute.String("schedule_id", scheduleID))

	payload, _ := json.Marshal(emailsDuePayload{ScheduleID: scheduleID})

	rec := &kgo.Record{
		Topic: TopicEmailsDue,
		Key:   append([]byte(nil), id[:]...),
		Value: payload,
	}
	kafka.InjectOtelHeaders(ctx, rec)

	start := time.Now()
	err := producer.Produce(ctx, rec)
	mProduceDuration.With(podLabels(podIndex)).Observe(time.Since(start).Seconds())
	if err != nil {
		mProduceFailures.With(podLabels(podIndex)).Inc()
		span.RecordError(err)
		logger.WithCtx(ctx, lg).Error("kafka produce failure",
			zap.Int("pod_index", podIndex),
			zap.String("schedule_id", scheduleID),
			zap.Error(err),
		)
	}
}
