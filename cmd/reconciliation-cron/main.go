// reconciliation-cron — Hatch's crash-recovery cron. Sweeps Postgres on an
// interval for schedule rows stranded by a crash and re-enqueues them onto
// emails.due. Runs as a long-lived Deployment (not a CronJob) so Prometheus can
// scrape its /metrics between sweeps. See internal/recon for the two passes.
package main

import (
	"fmt"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/recon"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() { service.Main("reconciliation-cron", run) }

func run(lg *zap.Logger) error {
	cfg, err := config.Load[recon.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "reconciliation-cron", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()
	tr := otel.Tracer("reconciliation-cron")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()

	go recon.Run(ctx, cfg, gen.New(pool), hkafka.NewRecordProducer(prodCl), tr, lg)

	return service.Serve(ctx, lg, "reconciliation-cron", cfg.AdminPort,
		recon.AdminHandler(pool, prodCl), cfg.ShutdownTimeout,
		zap.Duration("interval", cfg.Interval))
}
