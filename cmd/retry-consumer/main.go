// Command retry-consumer drains each retry tier back onto emails.due on that
// tier's interval.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/retry"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"go.uber.org/zap"
)

func main() {
	service.Run("retry-consumer", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[retry.Config]()
		if err != nil {
			return err
		}
		producer, err := kafka.NewProducer(cfg.KafkaBrokers, lg)
		if err != nil {
			return err
		}
		defer producer.Close()
		retries, err := retry.New(cfg, lg, producer)
		if err != nil {
			return err
		}
		defer retries.Close()

		health := httpx.NewRouter(httpx.Check{Name: "kafka", Ping: producer.Ping})
		return service.Serve(ctx, lg, cfg.Port, health, retries.Run)
	})
}
