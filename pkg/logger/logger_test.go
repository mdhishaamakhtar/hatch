package logger

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWithCtx_injectsTraceFields(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	core, logs := observer.New(zapcore.InfoLevel)
	base := zap.New(core)
	lg := WithCtx(ctx, base)
	lg.Info("event")

	fields := map[string]string{}
	for _, f := range logs.All()[0].Context {
		fields[f.Key] = f.String
	}
	if fields["trace_id"] != tid.String() {
		t.Errorf("trace_id = %q, want %q", fields["trace_id"], tid.String())
	}
	if fields["span_id"] != sid.String() {
		t.Errorf("span_id = %q, want %q", fields["span_id"], sid.String())
	}
}

func TestWithCtx_noSpan_returnsBase(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	base := zap.New(core)
	lg := WithCtx(context.Background(), base)
	lg.Info("event")
	if len(logs.All()[0].Context) != 0 {
		t.Errorf("expected no trace fields on bare context, got %+v", logs.All()[0].Context)
	}
}
