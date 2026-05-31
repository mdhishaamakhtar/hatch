// reconciliation-cron — Hatch Phase 5 service. On a fixed interval it runs two
// SQL passes that recover schedule rows stranded by a crash (stuck pending/
// processing/retrying) and re-enqueues each onto emails.due for the delivery
// worker. Idempotent: the worker's Redis SET NX dedups any double-send. See
// internal/recon for the design.
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
	"github.com/mdhishaamakhtar/hatch/internal/recon"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events BEFORE the logger exists.
	lg, err := logger.New("reconciliation-cron")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("reconciliation-cron startup failed", zap.Error(err))
	}
}

func run(lg *zap.Logger) error {
	cfg, err := config.Load[recon.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "reconciliation-cron", cfg.OTLPEndpoint)
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
	tr := otel.Tracer("reconciliation-cron")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()

	store := gen.New(pool)
	producer := recon.NewKgoProducer(prodCl)
	go recon.Run(ctx, store, producer, tr, lg, cfg.Interval, cfg.RunOnStart)

	srv := recon.NewServer(lg, pool, prodCl)
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
		lg.Info("reconciliation-cron listening", zap.Int("admin_port", cfg.AdminPort))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
		close(listenErr)
	}()

	select {
	case err := <-listenErr:
		return fmt.Errorf("http listen: %w", err)
	case <-ctx.Done():
		lg.Info("shutdown signal received")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutMS)*time.Millisecond)
	defer c()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	lg.Info("reconciliation-cron stopped cleanly")
	return nil
}
