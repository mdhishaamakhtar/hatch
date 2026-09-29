package retry

import (
	"context"
	"errors"
	"testing"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type fakeProducer struct {
	err      error
	produced []*kgo.Record
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	var out kgo.ProduceResults
	for _, r := range rs {
		f.produced = append(f.produced, r)
		out = append(out, kgo.ProduceResult{Record: r, Err: f.err})
	}
	return out
}

func testDrainer(p producer) *drainer {
	return &drainer{tier: kafka.RetryTiers[0], producer: p, lg: zap.NewNop()}
}

func TestReEnqueueMovesRecordsToEmailsDue(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "delivery attempt")
	defer span.End()
	id := uuid.New()
	parked := kafka.DueRecord(ctx, kafka.RetryTiers[0].Topic, id)

	prod := &fakeProducer{}
	if !testDrainer(prod).reEnqueue(context.Background(), []*kgo.Record{parked, {Value: []byte("garbage")}}) {
		t.Fatal("reEnqueue reported a failure")
	}

	if len(prod.produced) != 1 {
		t.Fatalf("produced %d records, want the one readable record", len(prod.produced))
	}
	out := prod.produced[0]
	if got, _ := kafka.ScheduleID(out); out.Topic != kafka.TopicDue || got != id {
		t.Errorf("produced %s to %s, want %s to %s", got, out.Topic, id, kafka.TopicDue)
	}
	// The retry continues the trace of the attempt that failed.
	if got := trace.SpanContextFromContext(kafka.ExtractTrace(context.Background(), out)); got.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("trace %s, want %s", got.TraceID(), span.SpanContext().TraceID())
	}
}

// A failed produce must be reported, so the drain leaves the batch
// uncommitted for the next cycle.
func TestReEnqueueReportsFailures(t *testing.T) {
	prod := &fakeProducer{err: errors.New("broker down")}
	parked := kafka.DueRecord(context.Background(), kafka.RetryTiers[0].Topic, uuid.New())
	if testDrainer(prod).reEnqueue(context.Background(), []*kgo.Record{parked}) {
		t.Fatal("reEnqueue reported success despite the produce failing")
	}
}

func TestNewRequiresAnIntervalPerTier(t *testing.T) {
	if _, err := New(Config{Intervals: nil}, zap.NewNop(), nil); err == nil {
		t.Fatal("New accepted a config without intervals")
	}
}
