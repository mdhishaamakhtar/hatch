package verify

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/archival"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"go.opentelemetry.io/otel"
)

// Isolated test partitions, both in the past and outside the pre-created runway
// (which starts at the current month and runs forward). The y2020m01 partition is
// fully terminal and must be archived; the y2019m01 partition holds a non-terminal
// row and must be skipped.
const (
	archiveReadyPartition    = "scheduled_emails_y2020m01"
	archiveNotReadyPartition = "scheduled_emails_y2019m01"
)

// checkArchival drives the real archival.ArchiveOnce against two isolated past
// partitions it creates from scratch, proving the eligibility gate and the
// detach→export→drop lifecycle without disturbing the 1200-partition runway:
//
//   - y2020m01 (fully terminal): detached, exported to a gzip CSV, and dropped.
//   - y2019m01 (one non-terminal row): left attached, skipped as not ready.
//
// The deployed partition-archival cron is configured dormant during the run (long
// interval) so only this in-process sweep acts on the test partitions.
func (v *Verifier) checkArchival(ctx context.Context) {
	v.rep.Section("Partition archival — isolated past partition detached + exported + dropped")

	if v.clientID == "" {
		v.rep.Fail("no verify client available; skipping archival check")
		return
	}
	clientUUID, err := uuid.Parse(v.clientID)
	if err != nil {
		v.rep.Failf("verify client id not a uuid: %v", err)
		return
	}
	clientBytes := db.UUIDToBytes(clientUUID)

	// Pre-clean any leftovers from a prior interrupted run (DROP TABLE removes a
	// partition whether it's attached or an orphaned detached table).
	_, _ = v.pool.Exec(ctx, `DROP TABLE IF EXISTS `+archiveReadyPartition)
	_, _ = v.pool.Exec(ctx, `DROP TABLE IF EXISTS `+archiveNotReadyPartition)

	// Create the two isolated past partitions.
	if _, err := v.pool.Exec(ctx,
		`CREATE TABLE `+archiveReadyPartition+` PARTITION OF scheduled_emails FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')`); err != nil {
		v.rep.Failf("create %s: %v", archiveReadyPartition, err)
		return
	}
	if _, err := v.pool.Exec(ctx,
		`CREATE TABLE `+archiveNotReadyPartition+` PARTITION OF scheduled_emails FOR VALUES FROM ('2019-01-01') TO ('2019-02-01')`); err != nil {
		v.rep.Failf("create %s: %v", archiveNotReadyPartition, err)
		return
	}
	// Always try to clean up the not-ready partition (the ready one is dropped by
	// archival itself on success).
	defer func() { _, _ = v.pool.Exec(ctx, `DROP TABLE IF EXISTS `+archiveNotReadyPartition) }()

	// Seed the ready partition with terminal rows (route by deliver_at into 2020-01)
	// and the not-ready partition with one non-terminal row (2019-01).
	readyIDs := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		id := db.UUIDToBytes(uuid.New())
		readyIDs = append(readyIDs, id)
		if _, err := v.pool.Exec(ctx,
			`INSERT INTO scheduled_emails
			   (id, client_id, deliver_at, status, recipient_email, from_email, subject, body)
			 VALUES ($1, $2, $3, 'delivered', $4, 'from@example.com', $5, '<p>archive</p>')`,
			id, clientBytes, time.Date(2020, 1, 15, 12, 0, 0, 0, time.UTC),
			"archive@example.com", v.runID+"-archive"); err != nil {
			v.rep.Failf("seed terminal row in %s: %v", archiveReadyPartition, err)
			return
		}
	}
	if _, err := v.pool.Exec(ctx,
		`INSERT INTO scheduled_emails
		   (id, client_id, deliver_at, status, recipient_email, from_email, subject, body)
		 VALUES ($1, $2, $3, 'pending', $4, 'from@example.com', $5, '<p>not-ready</p>')`,
		db.UUIDToBytes(uuid.New()), clientBytes, time.Date(2019, 1, 15, 12, 0, 0, 0, time.UTC),
		"notready@example.com", v.runID+"-notready"); err != nil {
		v.rep.Failf("seed non-terminal row in %s: %v", archiveNotReadyPartition, err)
		return
	}

	before := v.partitionCount(ctx)

	// Run the real archival sweep in-process, writing exports to a temp dir.
	archiveDir, err := os.MkdirTemp("", "hatch-archive-verify")
	if err != nil {
		v.rep.Failf("create temp archive dir: %v", err)
		return
	}
	defer func() { _ = os.RemoveAll(archiveDir) }()

	cfg := archival.Config{ArchiveDir: archiveDir}
	checked, archived, err := archival.ArchiveOnce(ctx, v.pool, gen.New(v.pool), cfg, otel.Tracer("verify"), v.lg)
	if err != nil {
		v.rep.Failf("ArchiveOnce: %v", err)
		return
	}
	v.rep.Check(archived >= 1 && checked >= 2,
		fmt.Sprintf("archival checked %d past partition(s) and archived %d", checked, archived),
		fmt.Sprintf("archival checked=%d archived=%d, want checked>=2 archived>=1", checked, archived))

	// The ready partition is detached AND dropped.
	v.rep.Check(!v.tableExists(ctx, archiveReadyPartition),
		archiveReadyPartition+" detached and dropped",
		archiveReadyPartition+" still exists after archival")

	// Its rows are gone from the live table.
	var remaining int
	_ = v.pool.QueryRow(ctx,
		`SELECT count(*) FROM scheduled_emails WHERE id = ANY($1::bytea[])`, readyIDs).Scan(&remaining)
	v.rep.Check(remaining == 0,
		"archived rows no longer queryable in scheduled_emails",
		fmt.Sprintf("%d archived rows still present in scheduled_emails", remaining))

	// The export file exists and is non-empty.
	exportPath := archiveDir + "/" + archiveReadyPartition + ".csv.gz"
	if info, statErr := os.Stat(exportPath); statErr == nil && info.Size() > 0 {
		v.rep.Passf("export written to %s (%d bytes, gzip CSV)", exportPath, info.Size())
	} else {
		v.rep.Failf("export %s missing or empty: %v", exportPath, statErr)
	}

	// The not-ready partition is left attached (skipped), and the runway is intact.
	v.rep.Check(v.tableExists(ctx, archiveNotReadyPartition),
		archiveNotReadyPartition+" left attached (non-terminal rows → skipped)",
		archiveNotReadyPartition+" was archived despite non-terminal rows")

	after := v.partitionCount(ctx)
	v.rep.Check(before-after == 1,
		fmt.Sprintf("exactly one partition removed (%d → %d); runway untouched", before, after),
		fmt.Sprintf("partition count delta = %d (%d → %d), want exactly 1 removed", before-after, before, after))

	// The deployed partition-archival cron's boot sweep must surface its gauges in
	// Prometheus — the last-run timestamp (staleness alert source) and the active-
	// partition count.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		n, _, err := v.promCount(ctx, `hatch_archival_last_run_timestamp`)
		return err == nil && n > 0
	}), "Prometheus has hatch_archival_last_run_timestamp (staleness alert source)",
		"Prometheus missing hatch_archival_last_run_timestamp — is the partition-archival cron scraped?")
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		n, val, err := v.promCount(ctx, `hatch_db_active_partitions`)
		return err == nil && n > 0 && val != "" && val != "0"
	}), "Prometheus has hatch_db_active_partitions > 0",
		"Prometheus missing hatch_db_active_partitions")
}

// partitionCount returns the number of partitions attached to scheduled_emails,
// or -1 on error.
func (v *Verifier) partitionCount(ctx context.Context) int {
	var n int
	if err := v.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'scheduled_emails'::regclass`).Scan(&n); err != nil {
		return -1
	}
	return n
}

// tableExists reports whether a relation by that name currently exists.
func (v *Verifier) tableExists(ctx context.Context, name string) bool {
	var exists bool
	if err := v.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
		return false
	}
	return exists
}
