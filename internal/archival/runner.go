package archival

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.uber.org/zap"
)

// Run archives on cfg.Interval until ctx is cancelled. It blocks; run it in a
// goroutine. A sweep's error is swallowed here — Archiver.Run already logged the
// specific failure — so the schedule keeps going.
func Run(ctx context.Context, a *Archiver) {
	a.lg.Info("archive directory configured", zap.String("archive_dir", a.cfg.ArchiveDir))
	service.RunCron(ctx, a.lg, "partition archival cron", a.cfg.Interval, a.cfg.RunOnStart, func(ctx context.Context) {
		a.lg.Info("archival run started")
		if _, _, err := a.Run(ctx); err != nil {
			a.lg.Error("archival run failed", zap.Error(err))
		}
	})
}
