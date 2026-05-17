// scheduler-api — Hatch Phase 1 service. Handles client schedule CRUD and
// admin client/provider provisioning. See internal/api for handler details.
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

	"github.com/mdhishaamakhtar/hatch/internal/api"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/redis"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events that occur BEFORE the logger exists
	// (i.e. logger init itself). Every other emission point uses lg.
	lg, err := logger.New("scheduler-api")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("scheduler-api startup failed", zap.Error(err))
	}
}

func run(lg *zap.Logger) error {
	cfg, err := config.Load[api.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "scheduler-api", cfg.OTLPEndpoint)
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

	cipher, err := api.LoadCipher(cfg.ProviderCredKey)
	if err != nil {
		return fmt.Errorf("cipher: %w", err)
	}

	srv := api.NewServer(cfg, lg, pool, rc, cipher)

	httpServer := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listenErr := make(chan error, 1)
	go func() {
		lg.Info("scheduler-api listening", zap.Int("port", cfg.Port))
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
	lg.Info("scheduler-api stopped cleanly")
	return nil
}
