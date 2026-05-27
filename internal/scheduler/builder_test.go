package scheduler

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

func TestBuilderAppendsToWheelAndStore(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	store := newFakeStore()
	w := NewWheel()

	in := make(chan Entry, 1)
	clear := make(chan string)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunBuilder(ctx, zap.NewNop(), in, clear, w, store, 0, tracer)
		close(done)
	}()

	deliverAt := time.Date(2030, 1, 1, 12, 3, 7, 0, time.UTC)
	in <- Entry{ID: id(0xab), DeliverAt: deliverAt}

	// Spin until the wheel reflects the append.
	for i := 0; i < 50; i++ {
		if _, total := w.Stats(); total == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if got := w.Drain(3, 7); len(got) != 1 || got[0] != id(0xab) {
		t.Fatalf("wheel slot 03:07 wrong: %v", got)
	}
	if got := store.snapshotData()["03:07"]; len(got) != 1 || got[0] != id(0xab) {
		t.Fatalf("bbolt slot 03:07 wrong: %v", got)
	}
}

func TestBuilderHandlesClearChannel(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	store := newFakeStore()
	_ = store.Append("05:05", id(1))
	store.deletes = nil
	w := NewWheel()

	in := make(chan Entry)
	clear := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunBuilder(ctx, zap.NewNop(), in, clear, w, store, 0, tracer)
		close(done)
	}()

	clear <- "05:05"
	for i := 0; i < 50; i++ {
		if len(store.deleteLog()) > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if _, present := store.snapshotData()["05:05"]; present {
		t.Fatal("expected 05:05 to be deleted from store")
	}
	if log := store.deleteLog(); len(log) != 1 || log[0] != "05:05" {
		t.Fatalf("Delete log = %v", log)
	}
}
