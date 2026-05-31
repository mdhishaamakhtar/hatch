package recon

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// TopicEmailsDue is the re-enqueue target — the same topic the scheduler fires
// onto and the retry consumers drain into. Defined here (rather than imported
// from internal/delivery) to keep the package decoupled; the name is part of the
// cross-service contract in the HLD.
const TopicEmailsDue = "emails.due"

// ReconcileOnce runs both reconciliation passes and re-enqueues every recovered
// schedule id onto emails.due, returning the per-pass recovery counts. The SQL
// passes (gen.ReconPass1FreshAttempt / ReconPass2OrphanedRetry) atomically flip
// the matching stuck rows to 'processing' and return their ids; this then
// produces one emails.due message per id, carrying a fresh OTel context.
func ReconcileOnce(ctx context.Context, store Store, producer Producer, tr trace.Tracer, lg *zap.Logger) (pass1, pass2 int, err error) {
	ctx, span := tr.Start(ctx, "recon.run")
	defer span.End()
	start := time.Now()

	p1, err := store.ReconPass1FreshAttempt(ctx)
	if err != nil {
		span.RecordError(err)
		lg.Error("postgres failure during recon", zap.String("pass", "pass1"), zap.Error(err))
		return 0, 0, err
	}
	pass1 = len(p1)
	produceRecovered(ctx, "pass1", rowIDs1(p1), producer, tr, lg)
	mRowsRecovered.WithLabelValues("pass1").Add(float64(pass1))

	p2, err := store.ReconPass2OrphanedRetry(ctx)
	if err != nil {
		span.RecordError(err)
		lg.Error("postgres failure during recon", zap.String("pass", "pass2"), zap.Error(err))
		return pass1, 0, err
	}
	pass2 = len(p2)
	produceRecovered(ctx, "pass2", rowIDs2(p2), producer, tr, lg)
	mRowsRecovered.WithLabelValues("pass2").Add(float64(pass2))

	dur := time.Since(start)
	span.SetAttributes(
		attribute.Int("pass1_count", pass1),
		attribute.Int("pass2_count", pass2),
		attribute.Float64("duration_seconds", dur.Seconds()),
	)
	mRunDuration.WithLabelValues().Observe(dur.Seconds())
	mLastRun.WithLabelValues().Set(float64(time.Now().Unix()))
	lg.Info("reconciliation run completed",
		zap.Int("pass1_count", pass1),
		zap.Int("pass2_count", pass2),
		zap.Int64("duration_ms", dur.Milliseconds()),
	)
	return pass1, pass2, nil
}

// produceRecovered re-enqueues each recovered id onto emails.due. A produce
// failure is logged (the row is already flipped to 'processing', so the next
// sweep re-enqueues it) but does not abort the pass.
func produceRecovered(ctx context.Context, pass string, ids [][]byte, producer Producer, tr trace.Tracer, lg *zap.Logger) {
	for _, idb := range ids {
		u, err := db.BytesToUUID(idb)
		if err != nil {
			lg.Error("recovered row has malformed id", zap.String("pass", pass), zap.Error(err))
			continue
		}
		sid := u.String()
		pctx, pspan := tr.Start(ctx, "kafka.produce.emails_due", trace.WithAttributes(attribute.String("pass", pass)))
		rec := &kgo.Record{Topic: TopicEmailsDue, Key: idb, Value: duePayload(sid)}
		kafka.InjectOtelHeaders(pctx, rec)
		if err := producer.Produce(pctx, rec); err != nil {
			pspan.RecordError(err)
			logger.WithCtx(pctx, lg).Error("kafka produce failure during recon",
				zap.String("schedule_id", sid), zap.Error(err))
		}
		pspan.End()
	}
}

func rowIDs1(rows []reconPass1Row) [][]byte {
	ids := make([][]byte, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids
}

func rowIDs2(rows []reconPass2Row) [][]byte {
	ids := make([][]byte, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids
}

// duePayload marshals the thin {schedule_id} envelope carried on emails.due.
func duePayload(scheduleID string) []byte {
	b, _ := json.Marshal(struct {
		ScheduleID string `json:"schedule_id"`
	}{scheduleID})
	return b
}
