// partition-archival — Hatch's partition-lifecycle cron. Sweeps the
// scheduled_emails partitions on an interval and, for each fully-past month
// whose rows are all terminal, detaches it, exports it to a gzip CSV, and drops
// it. Runs as a long-lived Deployment (not a CronJob) so Prometheus can scrape
// its /metrics between sweeps. See internal/archival for the design.
package main

import (
	"fmt"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/archival"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() { service.Main("partition-archival", run) }

func run(lg *zap.Logger) error {
	cfg, err := config.Load[archival.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "partition-archival", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()
	tr := otel.Tracer("partition-archival")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	go archival.Run(ctx, archival.NewArchiver(pool, gen.New(pool), cfg, tr, lg))

	return service.Serve(ctx, lg, "partition-archival", cfg.AdminPort,
		archival.AdminHandler(pool), cfg.ShutdownTimeout,
		zap.Duration("interval", cfg.Interval))
}
