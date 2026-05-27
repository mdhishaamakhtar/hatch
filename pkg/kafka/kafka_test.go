package kafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/twmb/franz-go/pkg/kgo"
)

// installPropagator registers TraceContext globally so Inject/Extract have
// something to encode. Tests run in isolation but the global is shared, so we
// always re-set rather than rely on package init order.
func installPropagator(t *testing.T) {
	t.Helper()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample())))
}

func TestInjectExtractRoundTrip(t *testing.T) {
	installPropagator(t)

	ctx, span := otel.Tracer("test").Start(context.Background(), "produce")
	defer span.End()

	rec := &kgo.Record{}
	InjectOtelHeaders(ctx, rec)

	var keys []string
	for _, h := range rec.Headers {
		keys = append(keys, h.Key)
	}
	gotTraceparent := false
	for _, k := range keys {
		if k == "traceparent" {
			gotTraceparent = true
		}
	}
	if !gotTraceparent {
		t.Fatalf("expected traceparent header, got %v", keys)
	}

	extracted := ExtractOtelHeaders(context.Background(), rec)
	sc := trace.SpanContextFromContext(extracted)
	if !sc.IsValid() {
		t.Fatal("extracted span context not valid")
	}
	if sc.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("trace id mismatch: got %s want %s", sc.TraceID(), span.SpanContext().TraceID())
	}
}

func TestSetReplacesExistingKey(t *testing.T) {
	rec := &kgo.Record{}
	c := headerCarrier{r: rec}
	c.Set("foo", "v1")
	c.Set("foo", "v2")
	if c.Get("foo") != "v2" {
		t.Fatalf("Get(foo) = %q want v2", c.Get("foo"))
	}
	if len(rec.Headers) != 1 {
		t.Fatalf("expected 1 header after replace, got %d", len(rec.Headers))
	}
}
