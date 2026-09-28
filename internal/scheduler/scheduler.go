// Package scheduler is the timer-wheel service. Each pod owns a hash slice of
// scheduled_emails. Every hour it loads the next hour of its slice into its
// wheel, and every second it publishes whatever has come due onto emails.due
// for the delivery workers.
package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

const (
	// pollInterval is how often the wheel is topped up, and lookahead how far
	// ahead each poll loads. The API refuses schedules due sooner than its
	// minimum horizon (an hour by default), so a schedule always exists by the
	// time the poll that covers it runs.
	pollInterval = time.Hour
	lookahead    = time.Hour

	// pollRetryDelay is how soon a failed poll is retried.
	pollRetryDelay = 10 * time.Second
)

// Config is read from the environment.
type Config struct {
	// This pod owns the schedules whose id hashes to PodIndex mod TotalPods.
	// POD_INDEX has no default: pods that all defaulted to 0 would each fire
	// the same shard, and no pod the others.
	PodIndex  int `env:"POD_INDEX,required,notEmpty"`
	TotalPods int `env:"TOTAL_PODS" envDefault:"1"`

	DatabaseURL  string   `env:"DATABASE_URL,required,notEmpty"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required,notEmpty"`
	AdminAPIKey  string   `env:"ADMIN_API_KEY,required,notEmpty"`
	WheelPath    string   `env:"SCHEDULER_WHEEL_DB_PATH" envDefault:"/var/lib/hatch/wheel.db"`
	Port         int      `env:"PORT" envDefault:"9022"`
}

var (
	tracer = otel.Tracer("scheduler")

	pollDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_scheduler_poll_duration_seconds",
		Help:    "Duration of one poll of Postgres for due schedules.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	pollLoaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_scheduler_poll_emails_loaded_total",
		Help: "Schedules loaded into the wheel by polls.",
	})
	wheelSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hatch_scheduler_wheel_total_loaded",
		Help: "Schedules in the wheel waiting to fire.",
	})
	fired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_scheduler_fired_total",
		Help: "Schedules published to emails.due.",
	})
	produceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_scheduler_kafka_produce_duration_seconds",
		Help:    "Latency of publishing one tick's due schedules to emails.due.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	produceFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_scheduler_kafka_produce_failures_total",
		Help: "Schedules that failed to publish and stay in the wheel for the next tick.",
	})
	shard = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hatch_scheduler_pod_index",
		Help: "Always 1; its labels say which shard this pod owns.",
	}, []string{"pod_index", "total_pods"})
)

// The scheduler's view of Postgres and Kafka, narrowed so tests can fake them.
type (
	dueLister interface {
		ListDue(ctx context.Context, arg db.ListDueParams) ([]db.ListDueRow, error)
	}
	producer interface {
		ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	}
)

// Scheduler runs one pod's poller and ticker over its wheel.
type Scheduler struct {
	cfg      Config
	lg       *zap.Logger
	queries  dueLister
	producer producer
	wheel    *wheel
	pollNow  chan struct{} // an on-demand poll; holds one request so they coalesce
}

// New opens the pod's wheel. The caller must Close the Scheduler.
func New(cfg Config, lg *zap.Logger, queries dueLister, producer producer) (*Scheduler, error) {
	if cfg.TotalPods < 1 || cfg.PodIndex < 0 || cfg.PodIndex >= cfg.TotalPods {
		return nil, fmt.Errorf("POD_INDEX %d is not a shard of TOTAL_PODS %d", cfg.PodIndex, cfg.TotalPods)
	}
	w, err := openWheel(cfg.WheelPath)
	if err != nil {
		return nil, fmt.Errorf("open wheel: %w", err)
	}
	shard.WithLabelValues(strconv.Itoa(cfg.PodIndex), strconv.Itoa(cfg.TotalPods)).Set(1)
	return &Scheduler{
		cfg:      cfg,
		lg:       lg.With(zap.Int("pod_index", cfg.PodIndex)),
		queries:  queries,
		producer: producer,
		wheel:    w,
		pollNow:  make(chan struct{}, 1),
	}, nil
}

// Close closes the wheel. Call it once Run has returned.
func (s *Scheduler) Close() error { return s.wheel.close() }

// Run polls and ticks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { s.runPoller(ctx) })
	s.runTicker(ctx)
	wg.Wait()
}

// runPoller keeps the wheel loaded a lookahead ahead of now. Each hourly poll
// starts where the previous one ended, so no deliver_at can fall between two
// polls, and where that is survives restarts along with the wheel: a pod that
// was down loads what it missed on its first poll.
//
// A poll requested through /internal/poll starts from now instead: its job is
// to find schedules created since the window they fall in was loaded.
func (s *Scheduler) runPoller(ctx context.Context) {
	from, err := s.wheel.loadedUntil()
	if err != nil || from.IsZero() {
		from = time.Now()
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var retry <-chan time.Time
		until := time.Now().Add(lookahead)
		if err := s.poll(ctx, from, until); err == nil {
			from = until
		} else if ctx.Err() == nil {
			s.lg.Error("poll failed", zap.Error(err))
			retry = time.After(pollRetryDelay)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-retry:
		case <-s.pollNow:
			if now := time.Now(); now.Before(from) {
				from = now
			}
		}
	}
}

// poll loads this pod's pending schedules due in (after, until] into the wheel.
func (s *Scheduler) poll(ctx context.Context, after, until time.Time) error {
	ctx, span := tracer.Start(ctx, "scheduler.poll")
	defer span.End()
	start := time.Now()

	rows, err := s.queries.ListDue(ctx, db.ListDueParams{
		After:     after,
		Until:     until,
		TotalPods: int32(s.cfg.TotalPods),
		PodIndex:  int32(s.cfg.PodIndex),
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	if err := s.wheel.load(rows, until); err != nil {
		span.RecordError(err)
		return fmt.Errorf("load wheel: %w", err)
	}

	pollDuration.Observe(time.Since(start).Seconds())
	pollLoaded.Add(float64(len(rows)))
	s.updateWheelSize()
	span.SetAttributes(attribute.Int("loaded", len(rows)))
	service.WithTrace(ctx, s.lg).Info("poll completed",
		zap.Time("after", after), zap.Time("until", until),
		zap.Int("loaded", len(rows)), zap.Duration("duration", time.Since(start)))
	return nil
}

// runTicker fires the wheel once a second, on the second.
func (s *Scheduler) runTicker(ctx context.Context) {
	// Start on a second boundary, so a schedule fires as its second begins
	// rather than wherever in the second the pod happened to boot.
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Until(time.Now().Truncate(time.Second).Add(time.Second))):
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		s.fire(ctx, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// fire publishes every schedule due by now, then removes what Kafka accepted
// from the wheel. Taking everything due, rather than exactly this second's
// schedules, means a tick delayed by a slow publish, or a schedule loaded
// after its second passed, still fires on the next tick.
//
// Removing only after the publish makes firing at-least-once: a crash in
// between fires the schedule again after restart, and the delivery worker
// drops the duplicate. A schedule Kafka rejected stays for the next tick.
func (s *Scheduler) fire(ctx context.Context, now time.Time) {
	keys, err := s.wheel.due(now)
	if err != nil {
		s.lg.Error("read wheel", zap.Error(err))
		return
	}
	if len(keys) == 0 {
		return
	}

	ctx, span := tracer.Start(ctx, "scheduler.fire")
	defer span.End()
	span.SetAttributes(attribute.Int("due", len(keys)))

	records := make([]*kgo.Record, 0, len(keys))
	keyOf := make(map[*kgo.Record][]byte, len(keys))
	for _, key := range keys {
		id, err := scheduleIDOf(key)
		if err != nil {
			s.lg.Error("unreadable wheel key", zap.Binary("key", key), zap.Error(err))
			continue
		}
		r := kafka.DueRecord(ctx, kafka.TopicDue, id)
		records = append(records, r)
		keyOf[r] = key
	}

	start := time.Now()
	results := s.producer.ProduceSync(ctx, records...)
	produceDuration.Observe(time.Since(start).Seconds())

	var sent [][]byte
	for _, res := range results {
		if res.Err != nil {
			produceFailures.Inc()
			span.RecordError(res.Err)
			continue
		}
		sent = append(sent, keyOf[res.Record])
	}
	if failed := len(records) - len(sent); failed > 0 {
		service.WithTrace(ctx, s.lg).Error("publish to emails.due failed; retrying next tick",
			zap.Int("failed", failed), zap.Error(results.FirstErr()))
	}
	if len(sent) == 0 {
		return
	}
	if err := s.wheel.remove(sent); err != nil {
		s.lg.Error("remove fired schedules from the wheel", zap.Error(err))
	}
	fired.Add(float64(len(sent)))
	s.updateWheelSize()
	service.WithTrace(ctx, s.lg).Info("wheel fired", zap.Int("count", len(sent)))
}

func (s *Scheduler) updateWheelSize() {
	if n, err := s.wheel.size(); err == nil {
		wheelSize.Set(float64(n))
	}
}
