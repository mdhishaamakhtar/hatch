// Command partition-archival periodically exports and drops the partitions of
// scheduled_emails whose month is over and whose schedules have all finished.
// It is a long-running process rather than a CronJob so that Prometheus can
// scrape it between sweeps.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/archival"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"go.uber.org/zap"
)

func main() {
	service.Run("partition-archival", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[archival.Config]()
		if err != nil {
			return err
		}
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()

		health := httpx.NewRouter(httpx.Check{Name: "postgres", Ping: pool.Ping})
		return service.Serve(ctx, lg, cfg.Port, health, func(ctx context.Context) {
			archival.Run(ctx, cfg.Interval, lg, pool, cfg.Dir)
		})
	})
}
