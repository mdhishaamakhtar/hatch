// Command delivery-worker consumes emails.due and sends each schedule through
// its client's providers.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/crypto"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/delivery"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	service.Run("delivery-worker", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[delivery.Config]()
		if err != nil {
			return err
		}
		cipher, err := crypto.New(cfg.ProviderCredKey)
		if err != nil {
			return err
		}
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		redis, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{cfg.RedisAddr}})
		if err != nil {
			return err
		}
		defer redis.Close()
		consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, delivery.ConsumerGroup, kafka.TopicDue, lg)
		if err != nil {
			return err
		}
		defer consumer.Close()
		producer, err := kafka.NewProducer(cfg.KafkaBrokers, lg)
		if err != nil {
			return err
		}
		defer producer.Close()

		worker := delivery.New(cfg, lg, pool, redis, cipher, consumer, producer)
		health := httpx.NewRouter(
			httpx.Check{Name: "postgres", Ping: pool.Ping},
			httpx.Check{Name: "redis", Ping: func(ctx context.Context) error {
				return redis.Do(ctx, redis.B().Ping().Build()).Error()
			}},
		)
		return service.Serve(ctx, lg, cfg.Port, health, worker.Run)
	})
}
