package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEveryRunsImmediatelyThenOnTheInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	done := make(chan struct{})
	go func() {
		Every(ctx, time.Millisecond, func(context.Context) { runs.Add(1) })
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for runs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runs.Load() < 3 {
		t.Fatalf("ran %d times, want repeated runs", runs.Load())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Every did not return after its context was cancelled")
	}
}

func TestEveryDoesNotWaitForTheFirstTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan struct{}, 1)
	go Every(ctx, time.Hour, func(context.Context) {
		select {
		case ran <- struct{}{}:
		default:
		}
	})

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("fn did not run before the first tick")
	}
}
