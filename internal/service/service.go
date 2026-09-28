// Package service is the process scaffolding every Hatch binary shares:
// structured logging, tracing, signal handling, and serving HTTP.
package service

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// shutdownTimeout bounds how long a stopping process waits for in-flight HTTP
// requests and for the final span export.
const shutdownTimeout = 10 * time.Second

// Run is the whole of a binary's main function. It builds the JSON logger every
// Hatch process shares, installs a tracer that exports to OTLP_ENDPOINT when that
// is set, and calls run with a context cancelled on SIGINT or SIGTERM. An error
// from run is logged and exits the process with status 1.
func Run(name string, run func(ctx context.Context, lg *zap.Logger) error) {
	lg, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, "create logger:", err)
		os.Exit(1)
	}
	lg = lg.With(zap.String("service", name))

	if err := start(name, lg, run); err != nil {
		lg.Error(name+" failed", zap.Error(err))
		_ = lg.Sync()
		os.Exit(1)
	}
	_ = lg.Sync()
}

func start(name string, lg *zap.Logger, run func(context.Context, *zap.Logger) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(name))),
	}
	if endpoint := os.Getenv("OTLP_ENDPOINT"); endpoint != "" {
		exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			return fmt.Errorf("trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		// ctx is cancelled by now, so the final flush gets a context of its own.
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			lg.Warn("flush traces", zap.Error(err))
		}
	}()

	return run(ctx, lg)
}

// Serve serves h on port, and runs each of background alongside it, until ctx
// is cancelled or the server fails. It then lets in-flight requests finish,
// cancels the context it gave background, and waits for them to return.
func Serve(ctx context.Context, lg *zap.Logger, port int, h http.Handler, background ...func(context.Context)) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for _, run := range background {
		wg.Go(func() { run(ctx) })
	}

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	lg.Info("listening", zap.Int("port", port))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	lg.Info("shutting down")
	drain, stop := context.WithTimeout(context.Background(), shutdownTimeout)
	defer stop()
	return srv.Shutdown(drain)
}

// Every calls fn now and then every interval until ctx is cancelled.
func Every(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		fn(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// WithTrace returns lg annotated with the trace and span ids of the span in
// ctx, so a log line in Loki links to its trace in Tempo.
func WithTrace(ctx context.Context, lg *zap.Logger) *zap.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return lg
	}
	return lg.With(zap.String("trace_id", sc.TraceID().String()), zap.String("span_id", sc.SpanID().String()))
}
