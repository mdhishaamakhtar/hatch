package scheduler

import (
	"context"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Entry is the bounded-channel payload from G1 → G2. cmd/scheduler owns the
// channel lifecycle; this type is just the message shape.
type Entry struct {
	ID        [16]byte
	DeliverAt time.Time
}

// RecordPodIdentity wires the pod-identity gauge once at boot so the
// scheduler-service dashboard can sanity-check sharding.
func RecordPodIdentity(podIndex, totalPods int) { recordPodIdentity(podIndex, totalPods) }

// RunPoller is G1. It polls Postgres at cfg.PollInterval (default 1h) for this
// pod's hash slice and forwards each (id, deliver_at) onto out via a
// non-blocking send. Drops on a full channel are surfaced as WARN logs;
// reconciliation owns recovery for dropped entries.
//
// The first poll fires immediately on entry — pod restart should not wait an
// hour to find this hour's work. Subsequent polls are spaced by cfg.PollInterval.
//
// tickC, if non-nil, replaces the internal ticker. Used by tests to drive
// poll cycles deterministically without a real time.Ticker.
//
// triggerC fires an out-of-band poll on demand (the POST /internal/poll admin
// endpoint sends on it). A nil triggerC simply never fires, so callers that
// don't wire one — and tests — are unaffected.
func RunPoller(
	ctx context.Context,
	lg *zap.Logger,
	cfg Config,
	q SchedulePoller,
	out chan<- Entry,
	tracer trace.Tracer,
	tickC <-chan time.Time,
	triggerC <-chan struct{},
) {
	if tickC == nil {
		t := time.NewTicker(cfg.PollInterval)
		defer t.Stop()
		tickC = t.C
	}

	// Immediate first poll.
	pollOnce(ctx, lg, cfg, q, out, tracer)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickC:
			pollOnce(ctx, lg, cfg, q, out, tracer)
		case <-triggerC:
			pollOnce(ctx, lg, cfg, q, out, tracer)
		}
	}
}

func pollOnce(
	ctx context.Context,
	lg *zap.Logger,
	cfg Config,
	q SchedulePoller,
	out chan<- Entry,
	tracer trace.Tracer,
) {
	ctx, span := tracer.Start(ctx, "scheduler.poll")
	defer span.End()

	span.SetAttributes(attribute.Int("pod_index", cfg.PodIndex))

	start := time.Now()
	lg = logger.WithCtx(ctx, lg)
	lg.Info("hourly poll started",
		zap.Int("pod_index", cfg.PodIndex),
	)

	rows, err := q.PollHourWindow(ctx, gen.PollHourWindowParams{
		TotalPods: int32(cfg.TotalPods),
		PodIndex:  int32(cfg.PodIndex),
	})
	if err != nil {
		lg.Error("hourly poll failed", zap.Error(err))
		span.RecordError(err)
		return
	}

	loaded, dropped := 0, 0
	for _, row := range rows {
		if len(row.ID) != 16 {
			lg.Warn("poll row id wrong length, skipping", zap.Int("len", len(row.ID)))
			continue
		}
		var id [16]byte
		copy(id[:], row.ID)
		entry := Entry{ID: id, DeliverAt: row.DeliverAt.Time}
		select {
		case out <- entry:
			loaded++
		default:
			dropped++
			lg.Warn("schedule channel full, dropping entry",
				zap.Int("pod_index", cfg.PodIndex),
				zap.String("schedule_id", uuidString(id)),
			)
		}
	}

	mPollEmailsLoaded.With(podLabels(cfg.PodIndex)).Add(float64(loaded))
	mPollDuration.With(podLabels(cfg.PodIndex)).Observe(time.Since(start).Seconds())

	span.SetAttributes(
		attribute.Int("rows_loaded", loaded),
		attribute.Int("rows_dropped", dropped),
	)
	lg.Info("hourly poll completed",
		zap.Int("pod_index", cfg.PodIndex),
		zap.Int("rows_loaded", loaded),
		zap.Int("rows_dropped", dropped),
		zap.Duration("duration", time.Since(start)),
	)
}
