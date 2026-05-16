package tracer

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestInit_noEndpoint_installsProvider(t *testing.T) {
	shutdown, err := Init(context.Background(), "probe", "")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	tr := otel.Tracer("test")
	_, span := tr.Start(context.Background(), "op")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("span context invalid — TracerProvider not installed")
	}
}

func TestInit_propagatesContext(t *testing.T) {
	shutdown, err := Init(context.Background(), "probe", "")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	ctx, span := otel.Tracer("test").Start(context.Background(), "outer")
	defer span.End()

	got := trace.SpanContextFromContext(ctx)
	if !got.IsValid() {
		t.Fatal("span context not in returned ctx")
	}
}
