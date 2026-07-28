package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// fakeProducer records the records it's asked to produce and can be made to fail.
type fakeProducer struct {
	got     []*kgo.Record
	failErr error
}

func (f *fakeProducer) Produce(_ context.Context, r *kgo.Record) error {
	f.got = append(f.got, r)
	return f.failErr
}

// testTracer installs a real (in-memory) tracer so Inject/Extract have
// something to encode; the drainer's tracing is part of what's under test.
func testTracer() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample())))
}

func testDrainer(producer kafka.Producer) *Drainer {
	testTracer()
	tier := Tier{Name: "1min", Topic: kafka.TopicRetry1Min, Group: "g"}
	return NewDrainer(tier, nil, producer, otel.Tracer("test"), zap.NewNop(), Config{})
}

func TestTiersAreDerivedFromConfig(t *testing.T) {
	cfg := Config{
		ConsumerGroupPrefix: "retry-consumer",
		Interval1Min:        time.Minute,
		Interval5Min:        5 * time.Minute,
		Interval30Min:       30 * time.Minute,
	}
	want := []Tier{
		{Name: "1min", Topic: kafka.TopicRetry1Min, Group: "retry-consumer-1min", Interval: time.Minute},
		{Name: "5min", Topic: kafka.TopicRetry5Min, Group: "retry-consumer-5min", Interval: 5 * time.Minute},
		{Name: "30min", Topic: kafka.TopicRetry30Min, Group: "retry-consumer-30min", Interval: 30 * time.Minute},
	}

	got := cfg.Tiers()

	if len(got) != len(want) {
		t.Fatalf("got %d tiers, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tier %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReEnqueueRecordRetargetsWithoutMutating(t *testing.T) {
	in := &kgo.Record{Topic: kafka.TopicRetry5Min, Key: []byte("k"), Value: []byte(`{"schedule_id":"abc"}`)}

	out := reEnqueueRecord(in)

	if out.Topic != kafka.TopicEmailsDue {
		t.Errorf("topic = %q, want %q", out.Topic, kafka.TopicEmailsDue)
	}
	if string(out.Key) != "k" || string(out.Value) != `{"schedule_id":"abc"}` {
		t.Errorf("key/value not preserved: key=%q value=%q", out.Key, out.Value)
	}
	// franz-go reuses record buffers, so the copy has to be real.
	in.Key[0] = 'x'
	if out.Key[0] == 'x' {
		t.Error("output key aliases the input; want a copy")
	}
}

func TestReEnqueueProducesToEmailsDueWithTraceContext(t *testing.T) {
	prod := &fakeProducer{}
	d := testDrainer(prod)
	recs := []*kgo.Record{
		{Topic: kafka.TopicRetry1Min, Key: []byte("1"), Value: kafka.MarshalDuePayload("a")},
		{Topic: kafka.TopicRetry1Min, Key: []byte("2"), Value: kafka.MarshalDuePayload("b")},
	}

	if ok := d.reEnqueue(context.Background(), recs); !ok {
		t.Fatal("reEnqueue reported failure on a clean produce")
	}

	if len(prod.got) != 2 {
		t.Fatalf("produced %d records, want 2", len(prod.got))
	}
	for _, r := range prod.got {
		if r.Topic != kafka.TopicEmailsDue {
			t.Errorf("re-enqueued to %q, want emails.due", r.Topic)
		}
		// Without the header the retry is a disconnected trace and you can't see
		// which delivery attempt it came from.
		if !hasHeader(r, "traceparent") {
			t.Error("re-enqueued record is missing its traceparent header")
		}
	}
}

// A produce failure must leave the batch uncommitted so the tier re-drains it
// next cycle — at-least-once, with Redis idempotency absorbing the duplicate.
func TestReEnqueueReportsFailureSoOffsetsAreNotCommitted(t *testing.T) {
	prod := &fakeProducer{failErr: errors.New("broker down")}
	d := testDrainer(prod)
	recs := []*kgo.Record{{Topic: kafka.TopicRetry1Min, Value: kafka.MarshalDuePayload("a")}}

	if ok := d.reEnqueue(context.Background(), recs); ok {
		t.Fatal("reEnqueue reported success despite a produce error")
	}
}

func hasHeader(r *kgo.Record, key string) bool {
	for _, h := range r.Headers {
		if h.Key == key {
			return true
		}
	}
	return false
}
