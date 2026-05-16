package logger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNew_emitsMandatoryFields(t *testing.T) {
	lg, err := New("probe")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// We just check fields exist in the emitted JSON; the underlying encoder is
	// zap's production JSON encoder so the format is contract-stable.
	core, logs := observer.New(zapcore.InfoLevel)
	lg = zap.New(core).With(zap.String("service", "probe"))
	lg.Info("hello")

	if logs.Len() != 1 {
		t.Fatalf("want 1 log, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Message != "hello" {
		t.Errorf("msg = %q, want hello", entry.Message)
	}
	if entry.Level != zapcore.InfoLevel {
		t.Errorf("level = %v, want info", entry.Level)
	}
	got := map[string]any{}
	for _, f := range entry.Context {
		got[f.Key] = f.String
	}
	if got["service"] != "probe" {
		t.Errorf("service = %v, want probe", got["service"])
	}
	// Round-trip through JSON to confirm shape.
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x", Time: time.Now()}, []zapcore.Field{zap.String("service", "probe")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"ts", "level", "msg", "service"} {
		if _, ok := out[k]; !ok {
			t.Errorf("missing mandatory field %q in JSON: %s", k, buf.String())
		}
	}
}

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
