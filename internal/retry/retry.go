// Package retry is the retry consumer. It drains each retry tier back onto
// emails.due on that tier's interval, which is what delays a failed send. It
// holds no state and decides nothing: when a schedule has run out of retries
// is up to the delivery worker.
package retry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Config is read from the environment.
type Config struct {
	KafkaBrokers []string `env:"KAFKA_BROKERS,required,notEmpty"`
	Port         int      `env:"PORT" envDefault:"9024"`

	// Intervals is how often each tier of kafka.RetryTiers is drained, in the
	// same order. A retry waits anywhere up to its tier's interval.
	Intervals []time.Duration `env:"RETRY_INTERVALS" envDefault:"1m,5m,30m"`
}

var (
	tracer = otel.Tracer("retry")

	drained = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_retry_drained_total",
		Help: "Schedules moved from a retry tier back onto emails.due.",
	}, []string{"tier"})
	reEnqueueFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_retry_reenqueue_failures_total",
		Help: "Schedules that failed to move back onto emails.due, by tier.",
	}, []string{"tier"})
	drainDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hatch_retry_drain_duration_seconds",
		Help:    "Time to drain one tier.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}, []string{"tier"})
)

type producer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// Consumer drains every retry tier.
type Consumer struct {
	drainers []*drainer
}

// A drainer owns one tier's topic, through a consumer group of its own.
type drainer struct {
	tier     kafka.RetryTier
	interval time.Duration
	consumer *kgo.Client
	producer producer
	lg       *zap.Logger
}

// New connects a consumer to each tier. The caller must Close it.
func New(cfg Config, lg *zap.Logger, producer *kgo.Client) (*Consumer, error) {
	if len(cfg.Intervals) != len(kafka.RetryTiers) {
		return nil, fmt.Errorf("RETRY_INTERVALS has %d intervals for %d tiers", len(cfg.Intervals), len(kafka.RetryTiers))
	}
	c := &Consumer{}
	for i, tier := range kafka.RetryTiers {
		consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, "retry-consumer-"+tier.Name, tier.Topic, lg)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.drainers = append(c.drainers, &drainer{
			tier:     tier,
			interval: cfg.Intervals[i],
			consumer: consumer,
			producer: producer,
			lg:       lg.With(zap.String("tier", tier.Name)),
		})
	}
	return c, nil
}

// Close closes the tiers' consumers.
func (c *Consumer) Close() {
	for _, d := range c.drainers {
		d.consumer.Close()
	}
}

// Run drains each tier every interval until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, d := range c.drainers {
		wg.Go(func() { service.Every(ctx, d.interval, d.drain) })
	}
	wg.Wait()
}

// drain moves everything waiting on the tier onto emails.due. A poll's records
// are committed only once all of them are re-enqueued; otherwise they stay for
// the next drain, and any duplicate that makes is dropped by the delivery
// worker.
func (d *drainer) drain(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "retry.drain", trace.WithAttributes(attribute.String("tier", d.tier.Name)))
	defer span.End()
	start := time.Now()

	total := 0
	for ctx.Err() == nil {
		// The tier counts as drained once a poll comes back empty for a second.
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		records := d.consumer.PollRecords(pollCtx, 1000).Records()
		cancel()
		if len(records) == 0 || !d.reEnqueue(ctx, records) {
			break
		}
		if err := d.consumer.CommitRecords(ctx, records...); err != nil {
			d.lg.Error("commit retry offsets", zap.Error(err))
			break
		}
		total += len(records)
	}

	drained.WithLabelValues(d.tier.Name).Add(float64(total))
	drainDuration.WithLabelValues(d.tier.Name).Observe(time.Since(start).Seconds())
	span.SetAttributes(attribute.Int("drained", total))
	if total > 0 {
		d.lg.Info("tier drained", zap.Int("count", total), zap.Duration("duration", time.Since(start)))
	}
}

// reEnqueue puts records back on emails.due, each continuing the trace of the
// send attempt that parked it. It reports whether every one made it.
func (d *drainer) reEnqueue(ctx context.Context, records []*kgo.Record) bool {
	due := make([]*kgo.Record, 0, len(records))
	for _, r := range records {
		id, err := kafka.ScheduleID(r)
		if err != nil {
			d.lg.Warn("dropping unreadable retry record", zap.Error(err))
			continue
		}
		due = append(due, kafka.DueRecord(kafka.ExtractTrace(ctx, r), kafka.TopicDue, id))
	}
	ok := true
	for _, res := range d.producer.ProduceSync(ctx, due...) {
		if res.Err != nil {
			reEnqueueFailures.WithLabelValues(d.tier.Name).Inc()
			d.lg.Error("re-enqueue to emails.due failed", zap.ByteString("schedule_id", res.Record.Value), zap.Error(res.Err))
			ok = false
		}
	}
	return ok
}
