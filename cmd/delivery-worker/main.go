// delivery-worker — Hatch Phase 3 service. Consumes `emails.due` from Kafka,
// hydrates each schedule from Postgres, routes the send through a provider
// (mock or Resend) behind a per-(client,vendor) circuit breaker + leaky bucket,
// and drives the scheduled_emails status machine to a terminal state. On
// transient failure it re-enqueues to the retry tiers. See internal/delivery for
// the 3-goroutine pipeline.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/delivery"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/provider"
	"github.com/mdhishaamakhtar/hatch/pkg/redis"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events BEFORE the logger exists.
	lg, err := logger.New("delivery-worker")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("delivery-worker startup failed", zap.Error(err))
	}
}

func run(lg *zap.Logger) error {
	cfg, err := config.Load[delivery.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "delivery-worker", cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("tracer: %w", err)
	}
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := tracerShutdown(shutdownCtx); err != nil {
			lg.Warn("tracer shutdown", zap.Error(err))
		}
	}()
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

	consumer, err := hkafka.NewConsumer(cfg.Brokers(), cfg.ConsumerGroup, []string{delivery.TopicEmailsDue}, lg)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()
	producer := delivery.NewKgoProducer(prodCl)

	// Vendor factories: only the providers implemented this phase. Resend uses
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
	cache := delivery.NewClientCache(rc, queries, cfg.ClientCacheTTL, lg)
	idem := delivery.NewIdempotency(rc, cfg.IdempotencyTTL)
	proc := delivery.NewProcessor(lg, queries, cache, idem, router, producer, tr, cfg.MaxRetries)

	// G1 → G2: one batch of lookahead. ackC unbuffered: the commit can't race
	// ahead of processing.
	batchC := make(chan delivery.Batch, 1)
	ackC := make(chan struct{})

	go delivery.RunConsumer(ctx, lg, consumer, cfg.BatchSize, batchC, ackC)
	go proc.Run(ctx, batchC, ackC)
	go delivery.RunRouterTicker(ctx, router, cfg.ProviderTick)

	srv := delivery.NewServer(lg, pool, rc)
	httpServer := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.AdminPort),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listenErr := make(chan error, 1)
	go func() {
		lg.Info("delivery-worker listening",
			zap.Int("admin_port", cfg.AdminPort),
			zap.String("consumer_group", cfg.ConsumerGroup),
			zap.Int("batch_size", cfg.BatchSize),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
		close(listenErr)
	}()

	select {
	case err := <-listenErr:
		return fmt.Errorf("http listen: %w", err)
	case <-ctx.Done():
		lg.Info("shutdown signal received, draining")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutMS)*time.Millisecond)
	defer c()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	lg.Info("delivery-worker stopped cleanly")
	return nil
}
