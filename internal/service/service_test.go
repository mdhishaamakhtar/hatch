package service

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestEveryRunsImmediatelyThenOnTheInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var runs atomic.Int32
		done := make(chan struct{})
		go func() {
			Every(ctx, time.Minute, func(context.Context) { runs.Add(1) })
			close(done)
		}()

		synctest.Sleep(2 * time.Minute)
		if got := runs.Load(); got != 3 {
			t.Fatalf("ran %d times in two intervals, want 3 (once at the start, once per tick)", got)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Every did not return after its context was cancelled")
		}
	})
}

func TestEveryDoesNotWaitForTheFirstTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int32
		go Every(t.Context(), time.Hour, func(context.Context) { runs.Add(1) })

		synctest.Wait()
		if runs.Load() != 1 {
			t.Fatal("fn did not run before the first tick")
		}
	})
}
