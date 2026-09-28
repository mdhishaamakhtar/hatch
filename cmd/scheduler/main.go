// Command scheduler runs one shard of Hatch's timer wheel: it loads its slice
// of upcoming schedules from Postgres and publishes each to emails.due the
// second it falls due.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/scheduler"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"go.uber.org/zap"
)

func main() {
	service.Run("scheduler-service", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[scheduler.Config]()
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

		s, err := scheduler.New(cfg, lg, db.New(pool), producer)
		if err != nil {
			return err
		}
		defer s.Close()
		return service.Serve(ctx, lg, cfg.Port, s.Handler(pool), s.Run)
	})
}
