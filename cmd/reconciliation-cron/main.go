// Command reconciliation-cron periodically puts schedules that a crash left
// stranded back on emails.due. It is a long-running process rather than a
// CronJob so that Prometheus can scrape it between sweeps.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/recon"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"go.uber.org/zap"
)

func main() {
	service.Run("reconciliation-cron", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[recon.Config]()
		if err != nil {
			return err
		}
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		producer, err := kafka.NewProducer(cfg.KafkaBrokers, lg)
		if err != nil {
			return err
		}
		defer producer.Close()

		health := httpx.NewRouter(
			httpx.Check{Name: "postgres", Ping: pool.Ping},
			httpx.Check{Name: "kafka", Ping: producer.Ping},
		)
		return service.Serve(ctx, lg, cfg.Port, health, func(ctx context.Context) {
			recon.Run(ctx, cfg.Interval, lg, db.New(pool), producer)
		})
	})
}
