package scheduler

import (
	"context"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Entry is one schedule handed from G1 to G2 for loading into the wheel.
type Entry struct {
	ID        [16]byte
	DeliverAt time.Time
}

// RunPoller is G1. It polls Postgres at cfg.PollInterval (default 1h) for this
// pod's hash slice and forwards each (id, deliver_at) onto the entries channel
// via a non-blocking send. Drops on a full channel are surfaced as WARN logs;
// reconciliation owns recovery for dropped entries.
//
// The first poll fires immediately on entry — a pod restart should not wait an
// hour to find this hour's work. After that the loop polls on the interval, or
// whenever the admin endpoint signals PollTrigger.
//
// tickC, if non-nil, replaces the internal ticker so tests can drive poll cycles
// deterministically.
func (p *Pipeline) RunPoller(ctx context.Context, tickC <-chan time.Time) {
	if tickC == nil {
		t := time.NewTicker(p.cfg.PollInterval)
		defer t.Stop()
		tickC = t.C
	}

	p.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickC:
			p.pollOnce(ctx)
		case <-p.pollNow:
			p.pollOnce(ctx)
		}
	}
}

func (p *Pipeline) pollOnce(ctx context.Context) {
	ctx, span := p.tracer.Start(ctx, "scheduler.poll")
	defer span.End()
	span.SetAttributes(attribute.Int("pod_index", p.cfg.PodIndex))

	start := time.Now()
	lg := logger.WithCtx(ctx, p.lg).With(zap.Int("pod_index", p.cfg.PodIndex))
	lg.Info("hourly poll started")

	rows, err := p.queries.PollHourWindow(ctx, gen.PollHourWindowParams{
		TotalPods: int32(p.cfg.TotalPods),
		PodIndex:  int32(p.cfg.PodIndex),
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
		select {
		case p.entries <- Entry{ID: id, DeliverAt: row.DeliverAt.Time}:
			loaded++
		default:
			dropped++
			lg.Warn("schedule channel full, dropping entry", zap.String("schedule_id", uuidString(id)))
		}
	}

	label := podLabel(p.cfg.PodIndex)
	mPollEmailsLoaded.WithLabelValues(label).Add(float64(loaded))
	mPollDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())

	span.SetAttributes(
		attribute.Int("rows_loaded", loaded),
		attribute.Int("rows_dropped", dropped),
	)
	lg.Info("hourly poll completed",
		zap.Int("rows_loaded", loaded),
		zap.Int("rows_dropped", dropped),
		zap.Duration("duration", time.Since(start)),
	)
}
