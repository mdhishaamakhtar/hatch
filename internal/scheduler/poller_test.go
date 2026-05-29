package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

type fakePoller struct {
	mu   sync.Mutex
	rows []gen.PollHourWindowRow
	hits int
}

func (f *fakePoller) PollHourWindow(_ context.Context, _ gen.PollHourWindowParams) ([]gen.PollHourWindowRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	return f.rows, nil
}

func mkRow(b byte, t time.Time) gen.PollHourWindowRow {
	return gen.PollHourWindowRow{
		ID:        []byte{b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b},
		DeliverAt: pgtype.Timestamptz{Time: t, Valid: true},
	}
}

func TestPollerForwardsRowsToChannel(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())
	tracer := noop.NewTracerProvider().Tracer("test")

	now := time.Now().Add(10 * time.Minute)
	p := &fakePoller{rows: []gen.PollHourWindowRow{mkRow(1, now), mkRow(2, now.Add(time.Minute))}}
	out := make(chan Entry, 8)

	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		RunPoller(ctx, zap.NewNop(), Config{PodIndex: 0, TotalPods: 1}, p, out, tracer, tick, nil)
		close(done)
	}()

	// The first poll fires immediately. Pull two entries.
	got := make([]Entry, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case e := <-out:
			got = append(got, e)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for poller output")
		}
	}
	cancel()
	<-done

	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].ID[0] != 1 || got[1].ID[0] != 2 {
		t.Fatalf("unexpected ids: %v", got)
	}
	if p.hits == 0 {
		t.Fatal("expected at least one poll hit")
	}
}

func TestPollerDropsOnFullChannel(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")

	rows := []gen.PollHourWindowRow{
		mkRow(1, time.Now()),
		mkRow(2, time.Now()),
		mkRow(3, time.Now()),
	}
	p := &fakePoller{rows: rows}
	out := make(chan Entry, 1) // intentionally tiny

	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		RunPoller(ctx, zap.NewNop(), Config{PodIndex: 0, TotalPods: 1}, p, out, tracer, tick, nil)
		close(done)
	}()

	// Take one entry — the poller's other rows should be dropped because
	// out is now full and we never drain again before cancelling.
	<-out
	cancel()
	<-done
	// Drain anything residual without blocking.
	select {
	case <-out:
	default:
	}
}

func TestPollerTickerFiresMultipleCycles(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	p := &fakePoller{rows: nil} // no rows; just count poll cycles
	out := make(chan Entry, 1)

	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time, 4)
	done := make(chan struct{})
	go func() {
		RunPoller(ctx, zap.NewNop(), Config{PodIndex: 0, TotalPods: 1}, p, out, tracer, tick, nil)
		close(done)
	}()

	// Push two ticks. With the immediate startup poll, that's 3 hits total.
	tick <- time.Now()
	tick <- time.Now()
	// Give the goroutine a moment to consume them. Poll loop is tight.
	for i := 0; i < 50 && pollerHits(p) < 3; i++ {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if pollerHits(p) < 3 {
		t.Fatalf("expected ≥3 poll cycles, got %d", pollerHits(p))
	}
}

func TestPollerTriggerFiresPoll(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	p := &fakePoller{rows: nil} // no rows; just count poll cycles
	out := make(chan Entry, 1)

	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	trigger := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		RunPoller(ctx, zap.NewNop(), Config{PodIndex: 0, TotalPods: 1}, p, out, tracer, tick, trigger)
		close(done)
	}()

	// Startup poll is hit #1. An out-of-band trigger should drive hit #2
	// without any tick.
	trigger <- struct{}{}
	for i := 0; i < 50 && pollerHits(p) < 2; i++ {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if pollerHits(p) < 2 {
		t.Fatalf("expected ≥2 poll cycles after trigger, got %d", pollerHits(p))
	}
}

func pollerHits(p *fakePoller) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits
}
