package service

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// RunCron invokes run on interval until ctx is cancelled, optionally once
// immediately at boot (runOnStart) so last-run gauges appear without waiting a
// full interval. name labels the lifecycle logs. It blocks; run it in a
// goroutine.
//
// run is expected to handle and log its own errors — a sweep failing must never
// stop the schedule.
func RunCron(ctx context.Context, lg *zap.Logger, name string, interval time.Duration, runOnStart bool, run func(context.Context)) {
	lg.Info(name+" started",
		zap.Duration("interval", interval),
		zap.Bool("run_on_start", runOnStart),
	)
	if runOnStart {
		run(ctx)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run(ctx)
		}
	}
}
