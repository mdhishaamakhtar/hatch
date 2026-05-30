// retry-consumer — Hatch Phase 4 service. Runs one drain goroutine per retry
// tier (emails.retry.1min / 5min / 30min): each drains its topic on a schedule
// and re-enqueues every schedule_id to emails.due with a fresh OTel context.
// Stateless — no Postgres or Redis. See internal/retry for the design.
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

	"github.com/mdhishaamakhtar/hatch/internal/retry"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events BEFORE the logger exists.
	lg, err := logger.New("retry-consumer")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("retry-consumer startup failed", zap.Error(err))
	}
}

func run(lg *zap.Logger) error {
	cfg, err := config.Load[retry.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "retry-consumer", cfg.OTLPEndpoint)
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
	tr := otel.Tracer("retry-consumer")

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()
	producer := retry.NewKgoProducer(prodCl)

	// One durable-group consumer + drain goroutine per tier.
	consumers := make([]interface{ Close() }, 0, 3)
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()
	for _, t := range cfg.Tiers() {
		consumer, err := hkafka.NewConsumer(cfg.Brokers(), t.Group, []string{t.Topic}, lg)
		if err != nil {
			return fmt.Errorf("kafka consumer (%s): %w", t.Name, err)
		}
		consumers = append(consumers, consumer)
		go retry.RunTier(ctx, t, consumer, producer, tr, lg, cfg.DrainBatchSize, cfg.FetchMaxWait)
	}

	srv := retry.NewServer(lg, prodCl)
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
		lg.Info("retry-consumer listening", zap.Int("admin_port", cfg.AdminPort))
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
	lg.Info("retry-consumer stopped cleanly")
	return nil
}
