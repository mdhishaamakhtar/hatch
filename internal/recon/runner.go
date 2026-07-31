package recon

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Run reconciles on cfg.Interval until ctx is cancelled. It blocks; run it in a
// goroutine. A sweep's error is swallowed here — ReconcileOnce already logged
// the specific pass/produce failure — so the schedule keeps going.
func Run(ctx context.Context, cfg Config, store Store, producer kafka.Producer, tr trace.Tracer, lg *zap.Logger) {
	service.RunCron(ctx, lg, "reconciliation cron", cfg.Interval, cfg.RunOnStart, func(ctx context.Context) {
		lg.Info("reconciliation run started")
		if _, _, err := ReconcileOnce(ctx, store, producer, tr, lg); err != nil {
			lg.Error("reconciliation run failed", zap.Error(err))
		}
	})
}
