package verify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/archival"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/recon"
)

// The crons run a sweep when they start and then rarely, so these checks run
// sweeps of their own, in process, over schedules they seed. From the deployed
// crons they only check the metrics of that first sweep.

// checkReconciliation strands two schedules the way a crash would, and checks
// a sweep puts both back on emails.due.
func (v *verifier) checkReconciliation(ctx context.Context) {
	v.section("Reconciliation")
	if v.clientKey == "" {
		v.fail("no client to seed schedules for")
		return
	}

	// Both have had two attempts. One then went unfired, so its attempts no
	// longer count; the other failed its third and was never re-enqueued.
	due := time.Now().Add(-10 * time.Minute)
	unfired, err := v.seed(ctx, due, db.ScheduleStatusPending, 2, time.Now())
	if err != nil {
		v.fail("seed a schedule: %v", err)
		return
	}
	orphaned, err := v.seed(ctx, due, db.ScheduleStatusRetrying, 2, time.Now().Add(-3*time.Hour))
	if err != nil {
		v.fail("seed a schedule: %v", err)
		return
	}

	since := time.Now()
	unattempted, orphanedRetries, err := recon.Sweep(ctx, v.lg, db.New(v.DB), v.producer)
	if err != nil {
		v.fail("sweep: %v", err)
		return
	}
	v.check(unattempted >= 1 && orphanedRetries >= 1,
		"the sweep recovered %d unattempted schedule(s) and %d orphaned retry(ies)", unattempted, orphanedRetries)
	rows, err := v.schedules(ctx, unfired, orphaned)
	if err != nil || len(rows) != 2 {
		v.fail("read the seeded schedules: %v", err)
		return
	}
	for _, row := range rows {
		switch uuid.UUID(row.ID) {
		case unfired:
			v.check(row.RetryCount == 0, "the unfired schedule's retry count was reset, to %d", row.RetryCount)
		case orphaned:
			v.check(row.RetryCount == 2, "the orphaned retry's retry count was kept, at %d", row.RetryCount)
		}
	}
	n := v.awaitDue(ctx, since, []uuid.UUID{unfired, orphaned}, 30*time.Second)
	v.check(n == 2, "%d of 2 recovered schedules put back on %s", n, kafka.TopicDue)
	v.checkMetric(ctx, `hatch_recon_last_run_timestamp`, 1)
}

// checkArchival checks a sweep archives a past month whose schedules have all
// finished, and keeps one that still has a schedule pending.
func (v *verifier) checkArchival(ctx context.Context) {
	v.section("Archival")
	if v.clientKey == "" {
		v.fail("no client to seed schedules for")
		return
	}

	// Two months older than any partition migration 004 creates.
	finished := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	unfinished := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, month := range []time.Time{finished, unfinished} {
		name := month.Format("scheduled_emails_y2006m01")
		_, err := v.DB.Exec(ctx, `DROP TABLE IF EXISTS `+name)
		if err == nil {
			_, err = v.DB.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF scheduled_emails FOR VALUES FROM ('%s') TO ('%s')`,
				name, month.Format(time.RFC3339), month.AddDate(0, 1, 0).Format(time.RFC3339)))
		}
		if err != nil {
			v.fail("create partition %s: %v", name, err)
			return
		}
		defer func() { _, _ = v.DB.Exec(context.WithoutCancel(ctx), `DROP TABLE IF EXISTS `+name) }()
	}
	for _, seed := range []struct {
		month  time.Time
		status db.ScheduleStatus
	}{
		{finished, db.ScheduleStatusDelivered},
		{finished, db.ScheduleStatusFailed},
		{unfinished, db.ScheduleStatusDelivered},
		{unfinished, db.ScheduleStatusPending},
	} {
		at := seed.month.AddDate(0, 0, 14)
		if _, err := v.seed(ctx, at, seed.status, 0, at); err != nil {
			v.fail("seed a schedule: %v", err)
			return
		}
	}

	dir, err := os.MkdirTemp("", "hatch-archive")
	if err != nil {
		v.fail("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	if _, _, err := archival.Sweep(ctx, v.lg, v.DB, dir); err != nil {
		v.fail("sweep: %v", err)
		return
	}

	name := finished.Format("scheduled_emails_y2006m01")
	v.check(!v.tableExists(ctx, name), "%s, whose schedules have all finished, was dropped", name)
	info, err := os.Stat(filepath.Join(dir, name+".csv.gz"))
	v.check(err == nil && info.Size() > 0, "%s was exported to %s.csv.gz first", name, name)
	name = unfinished.Format("scheduled_emails_y2006m01")
	v.check(v.tableExists(ctx, name), "%s, with a schedule still pending, was kept", name)

	v.checkMetric(ctx, `hatch_archival_last_run_timestamp`, 1)
	v.checkMetric(ctx, `hatch_db_active_partitions`, 1)
}

// seed inserts a schedule for the audit's client straight into the database.
func (v *verifier) seed(ctx context.Context, deliverAt time.Time, status db.ScheduleStatus, retryCount int16, updatedAt time.Time) (uuid.UUID, error) {
	id := db.NewScheduleID(deliverAt)
	_, err := v.DB.Exec(ctx, `
		INSERT INTO scheduled_emails
			(id, client_id, deliver_at, status, recipient_email, from_email, subject, body, retry_count, updated_at)
		VALUES ($1, $2, $3, $4, 'seeded@example.com', 'verify@hatch.test', $5, '', $6, $7)`,
		id[:], v.client[:], db.ScheduleDeliverAt(id), status, v.runID, retryCount, updatedAt)
	return id, err
}
