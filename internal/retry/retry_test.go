package retry

import (
	"context"
	"errors"
	"testing"
	"time"

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

func testTracer() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample())))
}

func TestTiers(t *testing.T) {
	cfg := Config{
		ConsumerGroupPrefix: "retry-consumer",
		Interval1Min:        time.Minute,
		Interval5Min:        5 * time.Minute,
		Interval30Min:       30 * time.Minute,
	}
	tiers := cfg.Tiers()
	if len(tiers) != 3 {
		t.Fatalf("got %d tiers, want 3", len(tiers))
	}
	want := []Tier{
		{Name: "1min", Topic: TopicRetry1Min, Group: "retry-consumer-1min", Interval: time.Minute},
		{Name: "5min", Topic: TopicRetry5Min, Group: "retry-consumer-5min", Interval: 5 * time.Minute},
		{Name: "30min", Topic: TopicRetry30Min, Group: "retry-consumer-30min", Interval: 30 * time.Minute},
	}
	for i, w := range want {
		if tiers[i] != w {
			t.Errorf("tier %d = %+v, want %+v", i, tiers[i], w)
		}
	}
}

func TestBrokers(t *testing.T) {
	c := Config{KafkaBrokers: " a:9092 , b:9092 ,"}
	got := c.Brokers()
	if len(got) != 2 || got[0] != "a:9092" || got[1] != "b:9092" {
		t.Fatalf("Brokers() = %v, want [a:9092 b:9092]", got)
	}
}

func TestReEnqueueRecord(t *testing.T) {
	in := &kgo.Record{Topic: TopicRetry5Min, Key: []byte("k"), Value: []byte(`{"schedule_id":"abc"}`)}
	out := reEnqueueRecord(in)
	if out.Topic != TopicEmailsDue {
		t.Errorf("topic = %q, want %q", out.Topic, TopicEmailsDue)
	}
	if string(out.Key) != "k" || string(out.Value) != `{"schedule_id":"abc"}` {
		t.Errorf("key/value not preserved: key=%q value=%q", out.Key, out.Value)
	}
	// Must be a copy, not an alias of the input slices.
	in.Key[0] = 'x'
	if out.Key[0] == 'x' {
		t.Error("output key aliases input key; want a copy")
	}
}

func TestScheduleIDFromValue(t *testing.T) {
	if got := scheduleIDFromValue([]byte(`{"schedule_id":"abc-123"}`)); got != "abc-123" {
		t.Errorf("got %q, want abc-123", got)
	}
	if got := scheduleIDFromValue([]byte(`not json`)); got != "" {
		t.Errorf("malformed payload should yield empty, got %q", got)
	}
}

func TestReEnqueueSuccess(t *testing.T) {
	testTracer()
	tr := otel.Tracer("test")
	fp := &fakeProducer{}
	recs := []*kgo.Record{
		{Topic: TopicRetry1Min, Key: []byte("1"), Value: []byte(`{"schedule_id":"a"}`)},
		{Topic: TopicRetry1Min, Key: []byte("2"), Value: []byte(`{"schedule_id":"b"}`)},
	}
	tier := Tier{Name: "1min", Topic: TopicRetry1Min}
	if ok := reEnqueue(context.Background(), tier, recs, fp, tr, zap.NewNop()); !ok {
		t.Fatal("reEnqueue returned false on clean produce")
	}
	if len(fp.got) != 2 {
		t.Fatalf("produced %d records, want 2", len(fp.got))
	}
	for _, r := range fp.got {
		if r.Topic != TopicEmailsDue {
			t.Errorf("re-enqueued to %q, want emails.due", r.Topic)
		}
		if !hasHeader(r, "traceparent") {
			t.Errorf("missing traceparent header on re-enqueued record")
		}
	}
}

func TestReEnqueueProduceFailure(t *testing.T) {
	testTracer()
	tr := otel.Tracer("test")
	fp := &fakeProducer{failErr: errors.New("broker down")}
	recs := []*kgo.Record{{Topic: TopicRetry1Min, Value: []byte(`{"schedule_id":"a"}`)}}
	tier := Tier{Name: "1min", Topic: TopicRetry1Min}
	if ok := reEnqueue(context.Background(), tier, recs, fp, tr, zap.NewNop()); ok {
		t.Fatal("reEnqueue returned true despite a produce error")
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
