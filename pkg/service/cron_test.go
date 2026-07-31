package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRunCronRunsImmediatelyWhenAsked(t *testing.T) {
	var runs atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A long interval means any run we observe came from runOnStart.
	go RunCron(ctx, zap.NewNop(), "test", time.Hour, true, func(context.Context) {
		runs.Add(1)
	})

	if !eventually(func() bool { return runs.Load() >= 1 }) {
		t.Fatal("runOnStart should have triggered a sweep before the first tick")
	}
}

func TestRunCronWaitsForTheFirstTickWhenNotAsked(t *testing.T) {
	var runs atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunCron(ctx, zap.NewNop(), "test", time.Hour, false, func(context.Context) {
		runs.Add(1)
	})

	time.Sleep(50 * time.Millisecond)
	if got := runs.Load(); got != 0 {
		t.Fatalf("ran %d times without runOnStart, want 0", got)
	}
}

func TestRunCronRepeatsOnTheInterval(t *testing.T) {
	var runs atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunCron(ctx, zap.NewNop(), "test", time.Millisecond, false, func(context.Context) {
		runs.Add(1)
	})

	if !eventually(func() bool { return runs.Load() >= 3 }) {
		t.Fatalf("expected repeated sweeps, got %d", runs.Load())
	}
}

func TestRunCronStopsOnContextCancel(t *testing.T) {
	var runs atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		RunCron(ctx, zap.NewNop(), "test", time.Millisecond, false, func(context.Context) {
			runs.Add(1)
		})
		close(done)
	}()

	eventually(func() bool { return runs.Load() >= 1 })
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunCron did not return after its context was cancelled")
	}
}

func eventually(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
