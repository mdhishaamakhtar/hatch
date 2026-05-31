package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/recon"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

// checkReconciliation drives the real recon.ReconcileOnce against two stranded
// rows it seeds, proving both passes recover the right rows with the right retry
// semantics and re-enqueue them onto emails.due:
//
//   - Pass 1 (fresh attempt): a pending row whose deliver_at has elapsed. recon
//     resets retry_count to 0.
//   - Pass 2 (orphaned retry): a retrying row whose updated_at is >2h old. recon
//     preserves retry_count.
//
// Both rows are seeded with retry_count=2; observing 0 vs 2 afterwards proves the
// reset-vs-preserve contrast. retry_count is asserted (not last_provider or the
// transient 'processing' status) because the live delivery worker consumes the
// re-enqueued ids and overwrites those — but a successful mock send never touches
// retry_count. The deployed reconciliation-cron is configured dormant during the
// run (long interval) so only this in-process sweep acts on the seeded rows.
func (v *Verifier) checkReconciliation(ctx context.Context) {
	v.rep.Section("Reconciliation — stuck rows recovered → emails.due")

	if v.clientID == "" {
		v.rep.Fail("no verify client available; skipping reconciliation check")
		return
	}
	clientUUID, err := uuid.Parse(v.clientID)
	if err != nil {
		v.rep.Failf("verify client id not a uuid: %v", err)
		return
	}
	clientBytes := db.UUIDToBytes(clientUUID)

	// Seed one stuck row per pass. deliver_at sits an hour in the past — inside the
	// current-month partition, which always exists.
	pendingID := uuid.New()
	retryingID := uuid.New()
	deliverAt := time.Now().Add(-time.Hour)

	if _, err := v.pool.Exec(ctx,
		`INSERT INTO scheduled_emails
		   (id, client_id, deliver_at, status, recipient_email, from_email, subject, body, retry_count, last_provider, updated_at)
		 VALUES ($1, $2, $3, 'pending', $4, 'from@example.com', $5, '<p>recon</p>', 2, 'seed-provider', now())`,
		db.UUIDToBytes(pendingID), clientBytes, deliverAt, "recon-pending@example.com", v.runID+"-recon-pending"); err != nil {
		v.rep.Failf("seed stuck pending row: %v", err)
		return
	}
	if _, err := v.pool.Exec(ctx,
		`INSERT INTO scheduled_emails
		   (id, client_id, deliver_at, status, recipient_email, from_email, subject, body, retry_count, last_provider, updated_at)
		 VALUES ($1, $2, $3, 'retrying', $4, 'from@example.com', $5, '<p>recon</p>', 2, 'resend', now() - interval '3 hours')`,
		db.UUIDToBytes(retryingID), clientBytes, deliverAt, "recon-retrying@example.com", v.runID+"-recon-retrying"); err != nil {
		v.rep.Failf("seed stuck retrying row: %v", err)
		return
	}

	// Position an at-end consumer on emails.due before the sweep, so we only
	// observe the fresh re-enqueues (not the topic's history).
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(v.cfg.Brokers...),
		kgo.ConsumeTopics(dueTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		v.rep.Failf("recon emails.due consumer: %v", err)
		return
	}
	defer cl.Close()
	posCtx, posCancel := context.WithTimeout(ctx, 5*time.Second)
	cl.PollFetches(posCtx)
	posCancel()

	// Run the real reconciliation sweep in-process against the cluster DB + Kafka.
	producer, err := hkafka.NewProducer(v.cfg.Brokers, v.lg)
	if err != nil {
		v.rep.Failf("recon kafka producer: %v", err)
		return
	}
	defer producer.Close()

	pass1, pass2, err := recon.ReconcileOnce(ctx, gen.New(v.pool), recon.NewKgoProducer(producer), otel.Tracer("verify"), v.lg)
	if err != nil {
		v.rep.Failf("ReconcileOnce: %v", err)
		return
	}
	v.rep.Check(pass1 >= 1 && pass2 >= 1,
		fmt.Sprintf("recon recovered rows in both passes (pass1=%d, pass2=%d)", pass1, pass2),
		fmt.Sprintf("recon did not recover both passes (pass1=%d, pass2=%d)", pass1, pass2))

	// Pass 1 resets retry_count to 0; pass 2 preserves it at the seeded 2. (Both
	// are stable under a subsequent successful mock delivery.)
	v.rep.Check(reconRetryCount(ctx, v, pendingID) == 0,
		"pass 1 reset retry_count to 0 on the recovered pending row",
		"pass 1 did not reset retry_count on the pending row")
	v.rep.Check(reconRetryCount(ctx, v, retryingID) == 2,
		"pass 2 preserved retry_count=2 on the recovered retrying row",
		"pass 2 did not preserve retry_count on the retrying row")

	// Both recovered ids must land on emails.due.
	want := map[string]bool{pendingID.String(): true, retryingID.String(): true}
	seen := make(map[string]bool, len(want))
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for len(seen) < len(want) {
		fetches := cl.PollFetches(cctx)
		if len(fetches.Errors()) > 0 {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if sid := jsonField(r.Value, "schedule_id"); want[sid] {
				seen[sid] = true
			}
		})
	}
	v.rep.Check(len(seen) == len(want),
		"both recovered schedule_ids re-enqueued onto emails.due",
		fmt.Sprintf("only %d of %d recovered ids reached emails.due", len(seen), len(want)))

	// The deployed reconciliation-cron's boot sweep must surface its last-run gauge
	// in Prometheus — this is what the staleness alert watches.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		n, _, err := v.promCount(ctx, `hatch_recon_last_run_timestamp`)
		return err == nil && n > 0
	}), "Prometheus has hatch_recon_last_run_timestamp (staleness alert source)",
		"Prometheus missing hatch_recon_last_run_timestamp — is the reconciliation-cron scraped?")
}

// reconRetryCount reads the retry_count for a seeded schedule id, returning -1 on
// any error so a failed read surfaces as a failed assertion.
func reconRetryCount(ctx context.Context, v *Verifier, id uuid.UUID) int16 {
	var rc int16
	if err := v.pool.QueryRow(ctx,
		`SELECT retry_count FROM scheduled_emails WHERE id = $1`, db.UUIDToBytes(id)).Scan(&rc); err != nil {
		return -1
	}
	return rc
}
