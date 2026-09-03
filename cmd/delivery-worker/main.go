// delivery-worker — Hatch's email sender. Consumes `emails.due` from Kafka,
// hydrates each schedule from Postgres, routes the send through a provider
// (mock or Resend) behind a per-(client,vendor) circuit breaker + leaky bucket,
// and drives the scheduled_emails status machine to a terminal state. On
// transient failure it re-enqueues to the retry tiers. See internal/delivery for
// the 3-goroutine pipeline.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/delivery"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/mdhishaamakhtar/hatch/pkg/redis"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() { service.Main("delivery-worker", run) }

func run(lg *zap.Logger) error {
	cfg, err := config.Load[delivery.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "delivery-worker", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()
	tr := otel.Tracer("delivery-worker")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	rc, err := redis.NewClient(cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rc.Close()

	cipher, err := crypto.LoadCipher(cfg.ProviderCredKey)
	if err != nil {
		return fmt.Errorf("cipher: %w", err)
	}

	consumer, err := hkafka.NewConsumer(cfg.Brokers(), cfg.ConsumerGroup, []string{hkafka.TopicEmailsDue}, lg)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()

	// Vendor factories: only the vendors with an implementation. Resend uses
	// per-client API keys decrypted from the cache; mock ignores credentials.
	factories := map[string]provider.Factory{
		"mock":   provider.MockFactory(cfg.Mock),
		"resend": provider.ResendFactory,
	}
	router := delivery.NewRouter(
		factories, cipher,
		cfg.ProviderRatePerSec, cfg.RefillPerTick(),
		cfg.BreakerMinRequests, cfg.BreakerFailureRatio, cfg.BreakerOpenTimeout,
	)

	queries := gen.New(pool)
	proc := delivery.NewProcessor(
		lg, queries,
		delivery.NewClientCache(rc, queries, cfg.ClientCacheTTL, lg),
		delivery.NewIdempotency(rc, cfg.IdempotencyTTL),
		router,
		hkafka.NewRecordProducer(prodCl),
		tr, cfg.MaxRetries,
	)

	// Two cancellation domains. SIGTERM cancels ctx, which stops polling for new
	// work; workCtx outlives it by the drain budget so the batch already in
	// flight finishes its sends and commits its offsets. Collapsing these into
	// one context makes every deploy that lands mid-send strand a row in
	// `processing` with its idempotency key already claimed.
	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	go func() {
		<-ctx.Done()
		time.AfterFunc(cfg.ShutdownTimeout, stopWork)
	}()

	// G1 → G2: one batch of lookahead. ackC unbuffered: the commit can't race
	// ahead of processing.
	batchC := make(chan delivery.Batch, 1)
	ackC := make(chan struct{})

	go delivery.RunConsumer(ctx, workCtx, lg, consumer, cfg.BatchSize, batchC, ackC)
	go proc.Run(workCtx, batchC, ackC)
	go delivery.RunRouterTicker(ctx, router, cfg.ProviderTick)

	return service.Serve(ctx, lg, "delivery-worker", cfg.AdminPort,
		delivery.AdminHandler(pool, rc), cfg.ShutdownTimeout,
		zap.String("consumer_group", cfg.ConsumerGroup),
		zap.Int("batch_size", cfg.BatchSize),
	)
}
