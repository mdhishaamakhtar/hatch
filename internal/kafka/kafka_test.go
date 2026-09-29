package kafka

import (
	"context"
	"testing"
	"uuid"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestDueRecordRoundTrip(t *testing.T) {
	id := uuid.New()
	r := DueRecord(context.Background(), TopicDue, id)

	if r.Topic != TopicDue {
		t.Errorf("topic = %q, want %q", r.Topic, TopicDue)
	}
	// The binary id is the key, so every record for a schedule shares a partition.
	if uuid.UUID(r.Key) != id {
		t.Errorf("key = %x, want the binary id %x", r.Key, id[:])
	}
	got, err := ScheduleID(r)
	if err != nil || got != id {
		t.Errorf("ScheduleID = %v, %v; want %v", got, err, id)
	}
}

func TestScheduleIDRejectsMalformedValues(t *testing.T) {
	for _, bad := range []string{"", "not-a-uuid", `{"schedule_id":"x"}`} {
		r := DueRecord(context.Background(), TopicDue, uuid.New())
		r.Value = []byte(bad)
		if _, err := ScheduleID(r); err == nil {
			t.Errorf("ScheduleID(%q) succeeded, want an error", bad)
		}
	}
}

func TestDueRecordCarriesTheTrace(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "produce")
	defer span.End()

	r := DueRecord(ctx, TopicDue, uuid.New())

	got := trace.SpanContextFromContext(ExtractTrace(context.Background(), r))
	if got.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("extracted trace %s, want %s", got.TraceID(), span.SpanContext().TraceID())
	}
}
