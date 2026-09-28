// Package delivery is the delivery worker. It consumes emails.due, sends each
// schedule through one of its client's providers, and moves the schedule's row
// to delivered, retrying, failed or cancelled.
//
// Delivery is at-least-once. A record is committed only after its send has
// finished, so a crash replays it; guarded status writes and a per-attempt
// claim in Redis make sure a replay doesn't send the same email twice.
package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/internal/crypto"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/provider"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/redis/rueidis"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	// ConsumerGroup is the Kafka consumer group the workers share emails.due in.
	ConsumerGroup = "delivery-workers"

	// batchSize caps how many records one poll of emails.due returns.
	batchSize = 1000

	// drainTimeout is how long a stopping worker keeps sending the batch it
	// already started. Cutting a send off midway would leave its row stuck in
	// processing until reconciliation found it.
	drainTimeout = 10 * time.Second
)

// Config is read from the environment.
type Config struct {
	DatabaseURL     string   `env:"DATABASE_URL,required,notEmpty"`
	KafkaBrokers    []string `env:"KAFKA_BROKERS,required,notEmpty"`
	RedisAddr       string   `env:"REDIS_ADDR,required,notEmpty"`
	ProviderCredKey string   `env:"PROVIDER_CRED_KEY,required,notEmpty"`
	Port            int      `env:"PORT" envDefault:"9023"`

	// SendConcurrency is how many sends a pod runs at once. A send is almost
	// all waiting on the provider, so a pod's throughput is roughly
	// SendConcurrency / provider latency.
	SendConcurrency int `env:"DELIVERY_SEND_CONCURRENCY" envDefault:"32"`

	Mock provider.MockConfig
}

var tracer = otel.Tracer("delivery")

// The worker's dependencies, as interfaces so tests can fake them.
type (
	store interface {
		GetSchedules(context.Context, db.GetSchedulesParams) ([]db.ScheduledEmail, error)
		MarkProcessing(context.Context, db.MarkProcessingParams) (int64, error)
		MarkDelivered(context.Context, db.MarkDeliveredParams) (int64, error)
		MarkRetrying(context.Context, db.MarkRetryingParams) (int64, error)
		MarkFailed(context.Context, db.MarkFailedParams) (int64, error)
		MarkCancelled(context.Context, db.MarkCancelledParams) (int64, error)
	}
	clientLookup interface {
		lookup(ctx context.Context, clientID uuid.UUID) (client, error)
	}
	claimer interface {
		claim(ctx context.Context, scheduleID uuid.UUID, attempt int16) (claimState, error)
		confirmSent(ctx context.Context, scheduleID uuid.UUID, attempt int16) error
	}
	producer interface {
		ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	}
)

// Worker consumes emails.due and delivers what it reads.
type Worker struct {
	lg          *zap.Logger
	consumer    *kgo.Client
	producer    producer
	store       store
	clients     clientLookup
	claims      claimer
	router      *router
	concurrency int
}

// New wires a worker to its dependencies.
func New(cfg Config, lg *zap.Logger, pool *pgxpool.Pool, redis rueidis.Client, cipher *crypto.Cipher, consumer, producer *kgo.Client) *Worker {
	queries := db.New(pool)
	return &Worker{
		lg:          lg,
		consumer:    consumer,
		producer:    producer,
		store:       queries,
		clients:     &clientCache{redis: redis, queries: queries, lg: lg},
		claims:      &claims{redis: redis},
		router:      newRouter(cipher, cfg.Mock),
		concurrency: max(cfg.SendConcurrency, 1),
	}
}

// Run consumes emails.due until ctx is cancelled. Each batch is committed only
// once every send in it has finished. When ctx is cancelled, polling stops but
// the batch underway gets drainTimeout to finish and commit.
func (w *Worker) Run(ctx context.Context) {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(drainTimeout, cancel) })
	defer stop()

	for {
		fetches := w.consumer.PollRecords(ctx, batchSize)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			w.lg.Warn("fetch error", zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
		})
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		w.processBatch(work, records)
		// Fails on its own if the drain ran out mid-batch, which is what should
		// happen: an unfinished batch must be read again.
		if err := w.consumer.CommitRecords(work, records...); err != nil {
			w.lg.Error("commit emails.due offsets", zap.Error(err))
		}
	}
}

// processBatch sends a batch's schedules, w.concurrency at a time. They are
// independent of each other: records are keyed by schedule id, so Kafka orders
// nothing between two different schedules.
func (w *Worker) processBatch(ctx context.Context, records []*kgo.Record) {
	start := time.Now()
	batchSizes.Observe(float64(len(records)))

	ids := make([][]byte, 0, len(records))
	deliverAts := make([]time.Time, 0, len(records))
	recordOf := make(map[uuid.UUID]*kgo.Record, len(records))
	for _, r := range records {
		id, err := kafka.ScheduleID(r)
		if err != nil {
			w.lg.Warn("skipping unreadable emails.due record", zap.Error(err))
			continue
		}
		ids = append(ids, id[:])
		deliverAts = append(deliverAts, db.ScheduleDeliverAt(id))
		recordOf[id] = r
	}

	rows, err := w.store.GetSchedules(ctx, db.GetSchedulesParams{Ids: ids, DeliverAts: deliverAts})
	for backoff := time.Second; err != nil; backoff = min(2*backoff, 30*time.Second) {
		w.lg.Error("fetch schedules; retrying", zap.Error(err), zap.Duration("in", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		rows, err = w.store.GetSchedules(ctx, db.GetSchedulesParams{Ids: ids, DeliverAts: deliverAts})
	}

	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup
	for _, row := range rows {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break // the drain ran out; the batch will not be committed
		}
		wg.Go(func() {
			defer func() { <-sem }()
			w.processOne(kafka.ExtractTrace(ctx, recordOf[uuid.UUID(row.ID)]), row)
		})
	}
	wg.Wait()
	batchDuration.Observe(time.Since(start).Seconds())
}

// processOne moves one schedule to its next state. Anything it cannot finish
// safely (Redis or Postgres failing, a send cut off by shutdown) it leaves in
// processing, where reconciliation picks it up.
func (w *Worker) processOne(ctx context.Context, row db.ScheduledEmail) {
	id := uuid.UUID(row.ID)
	ctx, span := tracer.Start(ctx, "delivery.process", trace.WithAttributes(attribute.String("schedule_id", id.String())))
	defer span.End()
	lg := service.WithTrace(ctx, w.lg).With(zap.Stringer("schedule_id", id))

	switch row.Status {
	case db.ScheduleStatusDelivered, db.ScheduleStatusFailed, db.ScheduleStatusCancelled:
		// A duplicate record for a schedule that is already finished.
		skipped.WithLabelValues(string(row.Status)).Inc()
		return
	}
	n, err := w.store.MarkProcessing(ctx, db.MarkProcessingParams{ID: row.ID, DeliverAt: row.DeliverAt})
	if !moved(lg, "processing", n, err) {
		return
	}

	c, err := w.clients.lookup(ctx, uuid.UUID(row.ClientID))
	if err != nil {
		lg.Warn("client lookup failed; leaving the row processing", zap.Error(err))
		return
	}
	if !c.Active {
		n, err := w.store.MarkCancelled(ctx, db.MarkCancelledParams{ID: row.ID, DeliverAt: row.DeliverAt, FailureReason: new("client_inactive")})
		if moved(lg, "cancelled", n, err) {
			cancelled.Inc()
		}
		return
	}

	switch claim, err := w.claims.claim(ctx, id, row.RetryCount); {
	case err != nil:
		idempotency.WithLabelValues("unavailable").Inc()
		lg.Warn("claiming the send failed; leaving the row processing", zap.Error(err))
		return
	case claim == claimSent:
		// Another worker sent this attempt and confirmed it; only the
		// bookkeeping is left.
		idempotency.WithLabelValues("duplicate_sent").Inc()
		w.markDelivered(ctx, lg, row, value(row.LastProvider))
		return
	case claim == claimInFlight:
		// Another worker holds this attempt and has not confirmed it: it may
		// still be sending, or it may have died mid-send. Marking the row
		// delivered could record an email that never went out, so leave it; a
		// dead worker's claim expires and reconciliation retries the row.
		idempotency.WithLabelValues("duplicate_in_flight").Inc()
		lg.Info("attempt claimed by another worker; leaving the row processing")
		return
	}
	idempotency.WithLabelValues("acquired").Inc()

	email := provider.Email{
		To:       row.RecipientEmail,
		From:     row.FromEmail,
		FromName: value(row.FromName),
		Subject:  row.Subject,
		Body:     row.Body,
	}
	vendor, err := w.router.send(ctx, uuid.UUID(row.ClientID), c.Providers, value(row.LastProvider), email)
	switch {
	case err == nil:
		sends.WithLabelValues(vendor, "success").Inc()
		if err := w.claims.confirmSent(ctx, id, row.RetryCount); err != nil {
			// The email is out either way; without the confirmation a duplicate
			// record could only send it once more.
			lg.Warn("confirming the send failed", zap.Error(err))
		}
		w.markDelivered(ctx, lg, row, vendor)
		e2eLatency.Observe(time.Since(row.DeliverAt).Seconds())
		lg.Info("email delivered", zap.String("provider", vendor))

	case errors.Is(err, errNoProvider):
		// A configuration problem rather than a blip: retrying cannot help.
		failed.WithLabelValues("no_active_providers").Inc()
		w.markFailed(ctx, lg, row, "", "no_active_providers")

	case errors.Is(err, errBreakerOpen), errors.Is(err, errNoCapacity):
		// Nothing was sent: every provider is unhealthy or at its rate limit.
		// Both pass on their own, so the send waits in a retry tier.
		deferred.WithLabelValues(err.Error()).Inc()
		w.retry(ctx, lg, row, "", err)

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Cut off by our own shutdown. Whether the email went out is unknown, and
		// a status write would fail on the same dead context.
		sends.WithLabelValues(vendor, "aborted").Inc()
		lg.Warn("send cut off by shutdown; leaving the row processing", zap.Error(err))

	case errors.Is(err, provider.ErrRateLimited):
		sends.WithLabelValues(vendor, "rate_limited").Inc()
		w.retry(ctx, lg, row, vendor, err)

	case errors.Is(err, provider.ErrTransient):
		sends.WithLabelValues(vendor, "transient").Inc()
		w.retry(ctx, lg, row, vendor, err)

	default:
		// Anything else, like credentials that no longer decrypt, won't fix
		// itself on a retry.
		sends.WithLabelValues(vendor, "permanent_error").Inc()
		failed.WithLabelValues("provider_error").Inc()
		w.markFailed(ctx, lg, row, vendor, "provider_error: "+err.Error())
	}
}

// retry parks the schedule on its next retry tier, or fails it once it has
// been through every tier.
func (w *Worker) retry(ctx context.Context, lg *zap.Logger, row db.ScheduledEmail, vendor string, cause error) {
	if int(row.RetryCount) >= len(kafka.RetryTiers) {
		failed.WithLabelValues("retry_exhausted").Inc()
		w.markFailed(ctx, lg, row, vendor, "retry_exhausted: "+cause.Error())
		return
	}
	n, err := w.store.MarkRetrying(ctx, db.MarkRetryingParams{
		ID:            row.ID,
		DeliverAt:     row.DeliverAt,
		LastProvider:  nullable(vendor),
		FailureReason: new(cause.Error()),
	})
	if !moved(lg, "retrying", n, err) {
		return
	}
	tier := kafka.RetryTiers[row.RetryCount]
	if err := w.producer.ProduceSync(ctx, kafka.DueRecord(ctx, tier.Topic, uuid.UUID(row.ID))).FirstErr(); err != nil {
		// The row stays retrying, and reconciliation re-enqueues orphaned retries.
		lg.Error("parking the retry failed", zap.String("tier", tier.Name), zap.Error(err))
		return
	}
	retries.WithLabelValues(tier.Name).Inc()
	lg.Info("email retrying", zap.String("provider", vendor), zap.String("tier", tier.Name))
}

func (w *Worker) markDelivered(ctx context.Context, lg *zap.Logger, row db.ScheduledEmail, vendor string) {
	n, err := w.store.MarkDelivered(ctx, db.MarkDeliveredParams{ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: nullable(vendor)})
	moved(lg, "delivered", n, err)
}

func (w *Worker) markFailed(ctx context.Context, lg *zap.Logger, row db.ScheduledEmail, vendor, reason string) {
	n, err := w.store.MarkFailed(ctx, db.MarkFailedParams{ID: row.ID, DeliverAt: row.DeliverAt, LastProvider: nullable(vendor), FailureReason: &reason})
	if moved(lg, "failed", n, err) {
		lg.Warn("email failed", zap.String("provider", vendor), zap.String("reason", reason))
	}
}

// moved reports whether a guarded status write changed the row. Changing
// nothing means the row moved first, say a cancel that raced the send, and its
// current state is the true one, so the caller stops.
func moved(lg *zap.Logger, to string, rows int64, err error) bool {
	switch {
	case err != nil:
		lg.Error("status write failed", zap.String("to", to), zap.Error(err))
		return false
	case rows == 0:
		lostRaces.WithLabelValues(to).Inc()
		lg.Warn("status changed underneath; leaving the row as it is", zap.String("attempted", to))
		return false
	}
	return true
}

// nullable maps "" to a NULL column.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// value reads a nullable column, with NULL as "".
func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
