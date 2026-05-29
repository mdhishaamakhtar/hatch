// scheduler-service — Hatch Phase 2 service. Polls Postgres for this pod's
// hash slice of pending schedules, incubates them in an in-memory wheel
// persisted to bbolt, and produces `emails.due` to Kafka at the exact second
// each schedule matures. See internal/scheduler for the goroutine pipeline.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/scheduler"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// fmt is only acceptable for events BEFORE the logger exists.
	lg, err := logger.New("scheduler-service")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := run(lg); err != nil {
		lg.Fatal("scheduler-service startup failed", zap.Error(err))
	}
}

// seedPodIndex populates POD_INDEX from the pod hostname when running inside a
// StatefulSet with a distroless image (no shell available for the wrapper trick).
// The hostname of a StatefulSet pod is always "<name>-<ordinal>".
func seedPodIndex() {
	if os.Getenv("POD_INDEX") != "" {
		return
	}
	host, err := os.Hostname()
	if err != nil {
		return
	}
	parts := strings.Split(host, "-")
	if len(parts) > 0 {
		_ = os.Setenv("POD_INDEX", parts[len(parts)-1])
	}
}

func run(lg *zap.Logger) error {
	seedPodIndex()
	cfg, err := config.Load[scheduler.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if cfg.TotalPods <= 0 {
		return fmt.Errorf("TOTAL_PODS must be > 0 (got %d)", cfg.TotalPods)
	}
	if cfg.PodIndex < 0 || cfg.PodIndex >= cfg.TotalPods {
		return fmt.Errorf("POD_INDEX %d out of range for TOTAL_PODS=%d", cfg.PodIndex, cfg.TotalPods)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tracerShutdown, err := tracer.Init(ctx, "scheduler-service", cfg.OTLPEndpoint)
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
	tr := otel.Tracer("scheduler-service")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	cl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer cl.Close()
	producer := scheduler.NewKgoProducer(cl)

	store, err := wheelstore.Open(cfg.WheelDBPath)
	if err != nil {
		return fmt.Errorf("wheelstore open: %w", err)
	}
	defer func() { _ = store.Close() }()

	wheel := scheduler.NewWheel()
	if err := scheduler.Recover(lg, wheel, store, time.Now()); err != nil {
		return fmt.Errorf("wheel recovery: %w", err)
	}

	queries := gen.New(pool)

	schedC := make(chan scheduler.Entry, cfg.ScheduleChannelBuffer)
	clearC := make(chan string, cfg.ClearChannelBuffer)
	// Buffered so the admin handler's non-blocking send always lands a pending
	// poll even if the poller is mid-cycle.
	pollTrigger := make(chan struct{}, 1)

	scheduler.RecordPodIdentity(cfg.PodIndex, cfg.TotalPods)

	go scheduler.RunPoller(ctx, lg, cfg, queries, schedC, tr, nil, pollTrigger)
	go scheduler.RunBuilder(ctx, lg, schedC, clearC, wheel, store, cfg.PodIndex, tr)
	go scheduler.RunTicker(ctx, lg, wheel, clearC, producer, cfg.PodIndex, tr, nil, nil)

	srv := scheduler.NewServer(cfg, lg, pool, wheel, func() bool { return true }, producer, pollTrigger)
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
		lg.Info("scheduler-service listening",
			zap.Int("admin_port", cfg.AdminPort),
			zap.Int("pod_index", cfg.PodIndex),
			zap.Int("total_pods", cfg.TotalPods),
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
	lg.Info("scheduler-service stopped cleanly")
	return nil
}
