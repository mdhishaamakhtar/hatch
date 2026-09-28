package scheduler

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type fakeLister struct {
	mu    sync.Mutex
	rows  []db.ListDueRow
	err   error
	calls []db.ListDueParams
}

func (f *fakeLister) ListDue(_ context.Context, arg db.ListDueParams) ([]db.ListDueRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, arg)
	return f.rows, f.err
}

func (f *fakeLister) call(i int) (db.ListDueParams, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i >= len(f.calls) {
		return db.ListDueParams{}, false
	}
	return f.calls[i], true
}

type fakeProducer struct {
	mu        sync.Mutex
	err       error
	published []uuid.UUID
}

func (p *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	defer p.mu.Unlock()
	var results kgo.ProduceResults
	for _, r := range rs {
		if r.Topic != kafka.TopicDue {
			panic("published to " + r.Topic)
		}
		if p.err == nil {
			id, _ := kafka.ScheduleID(r)
			p.published = append(p.published, id)
		}
		results = append(results, kgo.ProduceResult{Record: r, Err: p.err})
	}
	return results
}

func newTestScheduler(t *testing.T, lister dueLister, prod producer) *Scheduler {
	t.Helper()
	cfg := Config{TotalPods: 1, AdminAPIKey: testAdminKey, WheelPath: filepath.Join(t.TempDir(), "wheel.db")}
	s, err := New(cfg, zap.NewNop(), lister, prod)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// eventually polls cond instead of sleeping a fixed time, so the tests stay fast.
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewRejectsAShardOutsideThePods(t *testing.T) {
	for _, cfg := range []Config{{PodIndex: 2, TotalPods: 2}, {PodIndex: -1, TotalPods: 1}, {TotalPods: 0}} {
		cfg.WheelPath = filepath.Join(t.TempDir(), "wheel.db")
		if _, err := New(cfg, zap.NewNop(), &fakeLister{}, &fakeProducer{}); err == nil {
			t.Errorf("New accepted pod %d of %d", cfg.PodIndex, cfg.TotalPods)
		}
	}
}

func TestPollLoadsItsWindowIntoTheWheel(t *testing.T) {
	lister := &fakeLister{rows: []db.ListDueRow{row(time.Now().Add(time.Minute)), row(time.Now().Add(2 * time.Minute))}}
	s := newTestScheduler(t, lister, &fakeProducer{})
	after, until := time.Now(), time.Now().Add(time.Hour)

	if err := s.poll(context.Background(), after, until); err != nil {
		t.Fatalf("poll: %v", err)
	}

	if n, _ := s.wheel.size(); n != 2 {
		t.Errorf("wheel holds %d schedules, want 2", n)
	}
	if got, _ := s.wheel.loadedUntil(); !got.Equal(until) {
		t.Errorf("loadedUntil = %v, want %v", got, until)
	}
	if p, _ := lister.call(0); !p.After.Equal(after) || !p.Until.Equal(until) || p.TotalPods != 1 {
		t.Errorf("queried %+v, want the window (%v, %v] of shard 0/1", p, after, until)
	}
}

// A failed poll must not move the watermark, or its window would never be loaded.
func TestFailedPollLoadsNothing(t *testing.T) {
	s := newTestScheduler(t, &fakeLister{err: errors.New("db down")}, &fakeProducer{})

	if err := s.poll(context.Background(), time.Now(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("poll succeeded despite the query failing")
	}
	if got, _ := s.wheel.loadedUntil(); !got.IsZero() {
		t.Errorf("loadedUntil = %v after a failed poll, want unset", got)
	}
}

func TestFirePublishesWhatIsDueAndRemovesIt(t *testing.T) {
	now := time.Now()
	due, future := row(now.Add(-time.Second)), row(now.Add(time.Minute))
	prod := &fakeProducer{}
	s := newTestScheduler(t, &fakeLister{rows: []db.ListDueRow{due, future}}, prod)
	if err := s.poll(context.Background(), now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	s.fire(context.Background(), now)

	if len(prod.published) != 1 || prod.published[0] != uuid.UUID(due.ID) {
		t.Fatalf("published %v, want only the due schedule", prod.published)
	}
	if n, _ := s.wheel.size(); n != 1 {
		t.Errorf("wheel holds %d, want only the future schedule left", n)
	}
}

// A tick that runs late, or never runs, must not strand the schedules of the
// seconds it missed: the next tick fires everything due by then.
func TestFireCatchesUpOnMissedSeconds(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	rows := []db.ListDueRow{row(now.Add(-3 * time.Second)), row(now.Add(-2 * time.Second)), row(now)}
	prod := &fakeProducer{}
	s := newTestScheduler(t, &fakeLister{rows: rows}, prod)
	if err := s.poll(context.Background(), now.Add(-time.Minute), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	s.fire(context.Background(), now)

	if len(prod.published) != 3 {
		t.Fatalf("published %d schedules, want all 3 due by now", len(prod.published))
	}
}

func TestFireKeepsWhatKafkaRejectedForTheNextTick(t *testing.T) {
	now := time.Now()
	prod := &fakeProducer{err: errors.New("broker down")}
	s := newTestScheduler(t, &fakeLister{rows: []db.ListDueRow{row(now)}}, prod)
	if err := s.poll(context.Background(), now.Add(-time.Minute), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	s.fire(context.Background(), now)
	if n, _ := s.wheel.size(); n != 1 {
		t.Fatalf("wheel holds %d after a failed publish, want the schedule kept", n)
	}

	prod.err = nil
	s.fire(context.Background(), now.Add(time.Second))
	if len(prod.published) != 1 {
		t.Fatalf("published %d on the retry, want 1", len(prod.published))
	}
	if n, _ := s.wheel.size(); n != 0 {
		t.Errorf("wheel holds %d after the retry, want 0", n)
	}
}

// A restarted pod resumes loading from the end of what it loaded before, so
// schedules that came due while it was down are still found.
func TestPollerResumesFromTheWheelsWatermark(t *testing.T) {
	lister := &fakeLister{}
	s := newTestScheduler(t, lister, &fakeProducer{})
	loadedUntil := time.Now().Add(-10 * time.Minute)
	if err := s.wheel.load(nil, loadedUntil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runPoller(ctx)

	var first db.ListDueParams
	eventually(t, func() bool { p, ok := lister.call(0); first = p; return ok })
	if !first.After.Equal(loadedUntil) {
		t.Errorf("first poll after restart starts at %v, want the stored watermark %v", first.After, loadedUntil)
	}
}

// An on-demand poll rescans from now, where an hourly one would start at the
// watermark and miss schedules created since their window was loaded.
func TestOnDemandPollRescansFromNow(t *testing.T) {
	lister := &fakeLister{}
	s := newTestScheduler(t, lister, &fakeProducer{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runPoller(ctx)

	eventually(t, func() bool { _, ok := lister.call(0); return ok })
	s.pollNow <- struct{}{}
	var second db.ListDueParams
	eventually(t, func() bool { p, ok := lister.call(1); second = p; return ok })

	if first, _ := lister.call(0); !second.After.Before(first.Until) {
		t.Errorf("on-demand poll starts at %v, want a rescan from now, before the watermark %v", second.After, first.Until)
	}
}
