package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// fakeProducer collects every produced record. If err is non-nil, every
// Produce returns it so we can exercise the failure path.
type fakeProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
	err     error
}

func (p *fakeProducer) Produce(_ context.Context, r *kgo.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, r)
	return p.err
}

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

func TestTickerProducesAndClears(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	w := NewWheel()
	w.Append(7, 8, id(0x11))
	w.Append(7, 8, id(0x22))

	prod := &fakeProducer{}
	clear := make(chan string, 1)
	tick := make(chan time.Time, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunTicker(ctx, zap.NewNop(), w, clear, prod, 0, tracer, tick,
			func() time.Time {
				// Force the ticker to think it's 07:08 every time it asks.
				return time.Date(2030, 1, 1, 0, 7, 8, 0, time.UTC)
			})
		close(done)
	}()

	tick <- time.Now()

	// Wait for both messages to land.
	for i := 0; i < 100 && prod.count() < 2; i++ {
		time.Sleep(2 * time.Millisecond)
	}
	select {
	case k := <-clear:
		if k != "07:08" {
			t.Fatalf("clearC got %q want 07:08", k)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for clear signal")
	}
	cancel()
	<-done

	if got := prod.count(); got != 2 {
		t.Fatalf("produced %d records, want 2", got)
	}
	for _, r := range prod.records {
		if r.Topic != TopicEmailsDue {
			t.Errorf("wrong topic: %s", r.Topic)
		}
		if len(r.Value) == 0 {
			t.Error("empty value")
		}
	}
	if got := w.Drain(7, 8); len(got) != 0 {
		t.Fatalf("expected slot drained, got %d ids", len(got))
	}
}

func TestTickerRecordsProduceFailures(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	w := NewWheel()
	w.Append(0, 0, id(0x55))

	prod := &fakeProducer{err: errors.New("broker unavailable")}
	clear := make(chan string, 1)
	tick := make(chan time.Time, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// Use a unique pod_index label so the counter starts at 0 for this test
	// (other tests don't share state).
	const podIdx = 99
	before := testutil.ToFloat64(mProduceFailures.WithLabelValues("99"))
	go func() {
		RunTicker(ctx, zap.NewNop(), w, clear, prod, podIdx, tracer, tick,
			func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) })
		close(done)
	}()
	tick <- time.Now()
	for i := 0; i < 100 && prod.count() < 1; i++ {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	after := testutil.ToFloat64(mProduceFailures.WithLabelValues("99"))
	if after <= before {
		t.Fatalf("expected failure counter to increment: before=%v after=%v", before, after)
	}
}
