package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var errBrokerDown = errors.New("broker unavailable")

// counterValue reads the current value of a single metric series.
func counterValue(c prometheus.Counter) float64 { return testutil.ToFloat64(c) }

func TestPollerForwardsRowsToBuilder(t *testing.T) {
	due := time.Now().Add(10 * time.Minute)
	poller := &fakePoller{rows: []gen.PollHourWindowRow{pollRow(1, due), pollRow(2, due.Add(time.Minute))}}
	p := testPipeline(Config{}, newFakeStore(), poller, &fakeProducer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunPoller(ctx, make(chan time.Time)) // the startup poll fires on entry

	got := make([]Entry, 0, 2)
	for range 2 {
		select {
		case e := <-p.entries:
			got = append(got, e)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for poller output")
		}
	}
	if got[0].ID != id(1) || got[1].ID != id(2) {
		t.Fatalf("unexpected ids: %v", got)
	}
}

func TestPollerDropsWhenChannelIsFull(t *testing.T) {
	now := time.Now()
	poller := &fakePoller{rows: []gen.PollHourWindowRow{pollRow(1, now), pollRow(2, now), pollRow(3, now)}}
	cfg := Config{ScheduleChannelBuffer: 1}
	p := testPipeline(cfg, newFakeStore(), poller, &fakeProducer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunPoller(ctx, make(chan time.Time))

	// Only one entry fits; the rest are dropped rather than blocking the poll.
	// Reconciliation, not the poller, owns recovery for those.
	if !eventually(func() bool { return len(p.entries) == 1 }) {
		t.Fatalf("want exactly 1 buffered entry, got %d", len(p.entries))
	}
	<-p.entries
	time.Sleep(20 * time.Millisecond)
	if n := len(p.entries); n != 0 {
		t.Fatalf("dropped rows should not arrive later, got %d", n)
	}
}

func TestPollerRunsOnTickAndOnTrigger(t *testing.T) {
	poller := &fakePoller{} // no rows; we only count cycles
	p := testPipeline(Config{}, newFakeStore(), poller, &fakeProducer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time, 2)
	go p.RunPoller(ctx, tick)

	// The startup poll is cycle 1.
	if !eventually(func() bool { return poller.hitCount() >= 1 }) {
		t.Fatal("expected an immediate startup poll")
	}
	tick <- time.Now()
	if !eventually(func() bool { return poller.hitCount() >= 2 }) {
		t.Fatalf("tick did not drive a poll, hits=%d", poller.hitCount())
	}
	p.PollTrigger() <- struct{}{}
	if !eventually(func() bool { return poller.hitCount() >= 3 }) {
		t.Fatalf("trigger did not drive an out-of-band poll, hits=%d", poller.hitCount())
	}
}

func TestBuilderWritesWheelAndStoreTogether(t *testing.T) {
	store := newFakeStore()
	p := testPipeline(Config{}, store, &fakePoller{}, &fakeProducer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunBuilder(ctx)

	deliverAt := time.Date(2030, 1, 1, 12, 3, 7, 0, time.UTC)
	p.entries <- Entry{ID: id(0xab), DeliverAt: deliverAt}

	if !eventually(func() bool { _, total := p.wheel.Stats(); return total == 1 }) {
		t.Fatal("entry never reached the wheel")
	}
	if got := p.wheel.Drain(Slot{Min: 3, Sec: 7}); len(got) != 1 || got[0] != id(0xab) {
		t.Fatalf("wheel slot 03:07 = %v", got)
	}
	entries := store.snapshot()["03:07"]
	if len(entries) != 1 || entries[0].ID != id(0xab) {
		t.Fatalf("bbolt slot 03:07 = %v", entries)
	}
	// deliver_at must be persisted alongside the id — recovery depends on it to
	// tell a stale slot from a future one.
	if !entries[0].DeliverAt.Equal(deliverAt) {
		t.Errorf("persisted deliver_at = %v, want %v", entries[0].DeliverAt, deliverAt)
	}
}

func TestBuilderDeletesClearedSlots(t *testing.T) {
	store := newFakeStore()
	_ = store.Append("05:05", id(1), time.Now())
	p := testPipeline(Config{}, store, &fakePoller{}, &fakeProducer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunBuilder(ctx)

	p.cleared <- "05:05"

	if !eventually(func() bool { return len(store.deleteLog()) > 0 }) {
		t.Fatal("cleared slot was never deleted from the store")
	}
	if _, present := store.snapshot()["05:05"]; present {
		t.Fatal("05:05 should be gone from the store")
	}
}

func TestTickerProducesSlotThenClearsIt(t *testing.T) {
	prod := &fakeProducer{}
	p := testPipeline(Config{}, newFakeStore(), &fakePoller{}, prod)
	// Freeze the clock at 07:08 so the tick always drains that slot.
	p.now = func() time.Time { return time.Date(2030, 1, 1, 0, 7, 8, 0, time.UTC) }

	slot := Slot{Min: 7, Sec: 8}
	p.wheel.Append(slot, id(0x11))
	p.wheel.Append(slot, id(0x22))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time, 1)
	go p.RunTicker(ctx, tick)

	tick <- time.Now()

	select {
	case got := <-p.cleared:
		if got != "07:08" {
			t.Fatalf("cleared slot = %q, want 07:08", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the clear signal")
	}

	if got := prod.count(); got != 2 {
		t.Fatalf("produced %d records, want 2", got)
	}
	for _, r := range prod.snapshot() {
		if r.Topic != kafka.TopicEmailsDue {
			t.Errorf("topic = %q, want %q", r.Topic, kafka.TopicEmailsDue)
		}
		if kafka.ScheduleIDFromValue(r.Value) == "" {
			t.Errorf("record value is not a readable due payload: %s", r.Value)
		}
	}
	if got := p.wheel.Drain(slot); len(got) != 0 {
		t.Fatalf("slot should be empty after firing, got %d ids", len(got))
	}
}

func TestTickerCountsProduceFailures(t *testing.T) {
	prod := &fakeProducer{err: errBrokerDown}
	p := testPipeline(Config{PodIndex: 99}, newFakeStore(), &fakePoller{}, prod)
	p.now = func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) }
	p.wheel.Append(Slot{}, id(0x55))

	before := counterValue(mProduceFailures.WithLabelValues("99"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time, 1)
	go p.RunTicker(ctx, tick)
	tick <- time.Now()

	if !eventually(func() bool { return counterValue(mProduceFailures.WithLabelValues("99")) > before }) {
		t.Fatal("a failing produce should increment the failure counter")
	}
}
