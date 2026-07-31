// Package service holds the startup and shutdown scaffolding every Hatch
// cmd/ binary repeats: logger construction, signal-scoped context, tracer
// wiring, and serving an HTTP surface until SIGTERM.
package service

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

	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/mdhishaamakhtar/hatch/pkg/tracer"
	"go.uber.org/zap"
)

// tracerShutdownTimeout bounds the final span flush on exit.
const tracerShutdownTimeout = 5 * time.Second

// Main is the body of every cmd/<service>/main.go: build the logger, run fn,
// and turn a startup error into a fatal log. name is the service name used for
// the `service` log field and the OTel resource.
//
// fmt is used for the logger-init failure only — it is the one event that
// happens before a logger exists.
func Main(name string, fn func(*zap.Logger) error) {
	lg, err := logger.New(name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	if err := fn(lg); err != nil {
		lg.Fatal(name+" startup failed", zap.Error(err))
	}
}

// SignalContext returns a context cancelled on SIGINT/SIGTERM, plus its cancel.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// InitTracer installs the global tracer provider and returns a func the caller
// must defer — it flushes pending spans on a fresh context so shutdown still
// exports after ctx is cancelled.
func InitTracer(ctx context.Context, lg *zap.Logger, service, otlpEndpoint string) (func(), error) {
	shutdown, err := tracer.Init(ctx, service, otlpEndpoint)
	if err != nil {
		return nil, fmt.Errorf("tracer: %w", err)
	}
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), tracerShutdownTimeout)
		defer cancel()
		if err := shutdown(flushCtx); err != nil {
			lg.Warn("tracer shutdown", zap.Error(err))
		}
	}, nil
}

// Serve runs h on port until ctx is cancelled or the listener fails, then
// drains in-flight requests within shutdownTimeout. fields are logged with the
// "listening" line so each service can name its own startup details.
//
// It blocks — this is the last call in a service's run function.
func Serve(ctx context.Context, lg *zap.Logger, name string, port int, h http.Handler, shutdownTimeout time.Duration, fields ...zap.Field) error {
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listenErr := make(chan error, 1)
	go func() {
		lg.Info(name+" listening", append([]zap.Field{zap.Int("port", port)}, fields...)...)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	// A fresh context: ctx is already cancelled, and the drain needs its own budget.
	drainCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	lg.Info(name + " stopped cleanly")
	return nil
}
