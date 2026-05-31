package recon

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Run reconciles on interval until ctx is cancelled. When runOnStart is true it
// reconciles once immediately (before the first tick) so the last-run gauge and
// recovery counters appear without waiting a full interval. It blocks; run it in
// a goroutine.
func Run(ctx context.Context, store Store, producer Producer, tr trace.Tracer, lg *zap.Logger, interval time.Duration, runOnStart bool) {
	lg.Info("reconciliation cron started",
		zap.Duration("interval", interval),
		zap.Bool("run_on_start", runOnStart),
	)
	if runOnStart {
		reconcile(ctx, store, producer, tr, lg)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile(ctx, store, producer, tr, lg)
		}
	}
}

// reconcile runs one sweep, emitting the "run started" log and swallowing the
// error (ReconcileOnce already logged the specific pass/produce failure) so the
// ticker loop keeps going.
func reconcile(ctx context.Context, store Store, producer Producer, tr trace.Tracer, lg *zap.Logger) {
	lg.Info("reconciliation run started")
	if _, _, err := ReconcileOnce(ctx, store, producer, tr, lg); err != nil {
		lg.Error("reconciliation run failed", zap.Error(err))
	}
}
