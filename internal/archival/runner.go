package archival

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/gen"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Run archives on cfg.Interval until ctx is cancelled. When cfg.RunOnStart is
// true it sweeps once immediately (before the first tick) so the active-
// partitions and last-run gauges appear without waiting a full interval. It
// blocks; run it in a goroutine.
func Run(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg Config, tr trace.Tracer, lg *zap.Logger) {
	lg.Info("partition archival cron started",
		zap.Duration("interval", cfg.Interval),
		zap.Bool("run_on_start", cfg.RunOnStart),
		zap.String("archive_dir", cfg.ArchiveDir),
	)
	if cfg.RunOnStart {
		archive(ctx, pool, q, cfg, tr, lg)
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			archive(ctx, pool, q, cfg, tr, lg)
		}
	}
}

// archive runs one sweep, emitting the "run started" log and swallowing the
// error (ArchiveOnce already logged the specific failure) so the ticker keeps going.
func archive(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg Config, tr trace.Tracer, lg *zap.Logger) {
	lg.Info("archival run started")
	if _, _, err := ArchiveOnce(ctx, pool, q, cfg, tr, lg); err != nil {
		lg.Error("archival run failed", zap.Error(err))
	}
}
