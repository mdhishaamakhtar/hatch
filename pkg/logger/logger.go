// Package logger provides the project-wide zap logger factory.
//
// All Hatch services use the same production logger config so that Promtail
// can rely on a stable JSON shape in Loki: service, level, ts, msg, plus any
// contextual fields the caller adds with logger.With.
package logger

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// New returns a zap.Logger with the "service" field baked in.
// service must match one of the names listed in the Observability doc:
// scheduler-api, scheduler-service, delivery-worker, retry-consumer,
// reconciliation-cron, partition-archival.
func New(service string) (*zap.Logger, error) {
	lg, err := zap.NewProduction(zap.AddCallerSkip(0))
	if err != nil {
		return nil, err
	}
	return lg.With(zap.String("service", service)), nil
}

// WithCtx returns a child logger annotated with trace_id and span_id pulled
// from the OTel span carried by ctx. If ctx has no recording span, the logger
// is returned unchanged.
func WithCtx(ctx context.Context, lg *zap.Logger) *zap.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return lg
	}
	return lg.With(
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	)
}
