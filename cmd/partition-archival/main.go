// partition-archival — Hatch Phase 5 service. On a fixed interval it walks the
// scheduled_emails partitions and, for each one whose month is fully in the past
// and whose rows are all terminal, detaches it, exports it to a gzip CSV, and
// drops it to reclaim disk. Lightweight and idempotent; never touches the
// current/future runway. See internal/archival for the design.
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
	"github.com/mdhishaamakhtar/hatch/internal/archival"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events BEFORE the logger exists.
	lg, err := logger.New("partition-archival")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("partition-archival startup failed", zap.Error(err))
	}
}

func run(lg *zap.Logger) error {
	cfg, err := config.Load[archival.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "partition-archival", cfg.OTLPEndpoint)
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
	tr := otel.Tracer("partition-archival")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	q := gen.New(pool)
	go archival.Run(ctx, pool, q, cfg, tr, lg)

	srv := archival.NewServer(lg, pool)
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
		lg.Info("partition-archival listening", zap.Int("admin_port", cfg.AdminPort))
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
	lg.Info("partition-archival stopped cleanly")
	return nil
}
