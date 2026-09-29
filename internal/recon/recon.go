// Package recon is the reconciliation cron. It finds schedules a crash left
// stranded somewhere in the pipeline and puts them back on emails.due.
package recon

import (
	"context"
	"fmt"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Config is read from the environment.
type Config struct {
	DatabaseURL  string        `env:"DATABASE_URL,required,notEmpty"`
	KafkaBrokers []string      `env:"KAFKA_BROKERS,required,notEmpty"`
	Port         int           `env:"PORT" envDefault:"9025"`
	Interval     time.Duration `env:"RECON_INTERVAL" envDefault:"24h"`
}

var (
	tracer = otel.Tracer("recon")

	recovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hatch_recon_rows_recovered_total",
		Help: "Stranded schedules put back on emails.due, by the pass that found them.",
	}, []string{"pass"})
	sweepDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_recon_run_duration_seconds",
		Help:    "Time to run one reconciliation sweep.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})
	lastSweep = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hatch_recon_last_run_timestamp",
		Help: "Unix time of the last completed sweep. The staleness alert watches it.",
	})
)

// The sweep's view of Postgres and Kafka, narrowed so tests can fake them.
type (
	store interface {
		RecoverUnattempted(context.Context) ([][]byte, error)
		RecoverOrphanedRetries(context.Context) ([][]byte, error)
	}
	producer interface {
		ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	}
)

// Run sweeps now, then every interval, until ctx is cancelled.
func Run(ctx context.Context, interval time.Duration, lg *zap.Logger, store store, producer producer) {
	service.Every(ctx, interval, func(ctx context.Context) {
		if _, _, err := Sweep(ctx, lg, store, producer); err != nil {
			lg.Error("reconciliation sweep failed", zap.Error(err))
		}
	})
}

// Sweep recovers stranded schedules and re-enqueues them. Each recovery query
// moves the rows it finds to processing, so the next sweep leaves them alone
// unless they get stuck again. Re-enqueueing a schedule that turns out to be
// fine is harmless: the delivery worker drops duplicates.
func Sweep(ctx context.Context, lg *zap.Logger, store store, producer producer) (unattempted, orphaned int, err error) {
	ctx, span := tracer.Start(ctx, "recon.sweep")
	defer span.End()
	start := time.Now()

	ids, err := store.RecoverUnattempted(ctx)
	if err != nil {
		span.RecordError(err)
		return 0, 0, fmt.Errorf("recover unattempted schedules: %w", err)
	}
	unattempted = requeue(ctx, lg, producer, "unattempted", ids)

	ids, err = store.RecoverOrphanedRetries(ctx)
	if err != nil {
		span.RecordError(err)
		return unattempted, 0, fmt.Errorf("recover orphaned retries: %w", err)
	}
	orphaned = requeue(ctx, lg, producer, "orphaned_retry", ids)

	sweepDuration.Observe(time.Since(start).Seconds())
	lastSweep.SetToCurrentTime()
	span.SetAttributes(attribute.Int("unattempted", unattempted), attribute.Int("orphaned_retries", orphaned))
	service.WithTrace(ctx, lg).Info("reconciliation sweep completed",
		zap.Int("unattempted", unattempted), zap.Int("orphaned_retries", orphaned), zap.Duration("duration", time.Since(start)))
	return unattempted, orphaned, nil
}

// requeue puts the schedules one pass recovered on emails.due, returning how
// many there were. A schedule that fails to publish stays in processing, and
// the next sweep finds it again.
func requeue(ctx context.Context, lg *zap.Logger, producer producer, pass string, ids [][]byte) int {
	records := make([]*kgo.Record, 0, len(ids))
	for _, id := range ids {
		records = append(records, kafka.DueRecord(ctx, kafka.TopicDue, uuid.UUID(id)))
	}
	for _, res := range producer.ProduceSync(ctx, records...) {
		if res.Err != nil {
			service.WithTrace(ctx, lg).Error("re-enqueue failed",
				zap.String("pass", pass), zap.ByteString("schedule_id", res.Record.Value), zap.Error(res.Err))
		}
	}
	recovered.WithLabelValues(pass).Add(float64(len(ids)))
	return len(ids)
}
