package archival

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/gen"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// parentTable is the partitioned root whose children this service archives.
const parentTable = "scheduled_emails"

// Archiver owns one archival sweep's dependencies. Every step needs the same
// pool, queries, config, tracer, and logger, so they live here rather than
// being threaded through six-argument function signatures.
type Archiver struct {
	pool    *pgxpool.Pool
	queries *gen.Queries
	cfg     Config
	tracer  trace.Tracer
	lg      *zap.Logger
}

// NewArchiver wires a sweep. main owns the pool's lifecycle.
func NewArchiver(pool *pgxpool.Pool, queries *gen.Queries, cfg Config, tracer trace.Tracer, lg *zap.Logger) *Archiver {
	return &Archiver{pool: pool, queries: queries, cfg: cfg, tracer: tracer, lg: lg}
}

// Run walks every attached partition of scheduled_emails and, for each one whose
// month is fully in the past, archives it iff all its rows are terminal
// (delivered/failed/cancelled): detach → export to <ArchiveDir>/<name>.csv.gz →
// drop. The pre-created current/future runway is never touched (its months are
// not fully past). Returns how many fully-past partitions were checked and how
// many of those were archived. One partition's failure is logged and skipped; it
// does not abort the sweep.
func (a *Archiver) Run(ctx context.Context) (checked, archived int, err error) {
	ctx, span := a.tracer.Start(ctx, "archival.run")
	defer span.End()
	start := time.Now()

	if err := os.MkdirAll(a.cfg.ArchiveDir, 0o755); err != nil {
		span.RecordError(err)
		a.lg.Error("create archive dir failed", zap.String("archive_dir", a.cfg.ArchiveDir), zap.Error(err))
		return 0, 0, err
	}

	// A crash between DETACH and DROP leaves an orphan: still a real table with
	// real rows, but no longer a child of the parent, so listPartitions can never
	// see it again. Finish those first or they sit in the database forever.
	if err := a.finishDetached(ctx); err != nil {
		span.RecordError(err)
	}

	names, err := a.listPartitions(ctx)
	if err != nil {
		span.RecordError(err)
		a.lg.Error("list partitions failed", zap.Error(err))
		return 0, 0, err
	}

	now := time.Now()
	for _, name := range names {
		year, month, ok := parsePartitionMonth(name)
		if !ok || !isFullyPast(year, month, now) {
			continue
		}
		checked++
		done, err := a.archivePartition(ctx, name)
		if err != nil {
			span.RecordError(err)
			continue
		}
		if done {
			archived++
		}
	}

	dur := time.Since(start)
	span.SetAttributes(
		attribute.Int("partitions_checked", checked),
		attribute.Int("partitions_archived", archived),
		attribute.Float64("duration_seconds", dur.Seconds()),
	)
	mArchived.Add(float64(archived))
	mRunDuration.Observe(dur.Seconds())
	mLastRun.Set(float64(time.Now().Unix()))
	if active, err := a.countPartitions(ctx); err == nil {
		mActivePartitions.Set(float64(active))
	}
	a.lg.Info("archival run completed",
		zap.Int("partitions_checked", checked),
		zap.Int("partitions_archived", archived),
		zap.Int64("duration_ms", dur.Milliseconds()),
	)
	return checked, archived, nil
}

// archivePartition checks one fully-past partition for terminal readiness and,
// if ready, archives it (detach → export → drop). Returns (true, nil) when the
// partition was archived, (false, nil) when it was skipped as not ready, and a
// non-nil error on a Postgres/IO failure.
func (a *Archiver) archivePartition(ctx context.Context, name string) (bool, error) {
	ready, err := a.isReady(ctx, name)
	if err != nil || !ready {
		return false, err
	}

	ctx, span := a.tracer.Start(ctx, "archival.partition.archive",
		trace.WithAttributes(attribute.String("partition_name", name)))
	defer span.End()

	ident := pgx.Identifier{name}.Sanitize()

	// Detach — instant, no lock on active partitions.
	if _, err := a.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", parentTable, ident)); err != nil {
		span.RecordError(err)
		a.lg.Error("partition detach failed", zap.String("partition_name", name), zap.Error(err))
		return false, err
	}

	rowCount, path, err := a.exportAndDrop(ctx, name, ident)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	span.SetAttributes(attribute.Int64("row_count", rowCount), attribute.String("archive_path", path))
	a.lg.Info("partition archived",
		zap.String("partition_name", name),
		zap.Int64("row_count", rowCount),
		zap.String("archive_path", path),
	)
	return true, nil
}

// isReady reports whether every row in the partition has reached a terminal
// state. A partition with live rows is left in place and retried next cycle.
func (a *Archiver) isReady(ctx context.Context, name string) (bool, error) {
	ctx, span := a.tracer.Start(ctx, "archival.partition.check",
		trace.WithAttributes(attribute.String("partition_name", name)))
	defer span.End()

	nonTerminal, err := a.queries.CheckPartitionTerminal(ctx, name)
	if err != nil {
		span.RecordError(err)
		a.lg.Error("partition terminal check failed", zap.String("partition_name", name), zap.Error(err))
		return false, err
	}
	span.SetAttributes(attribute.Int64("non_terminal_count", nonTerminal))
	if nonTerminal > 0 {
		a.lg.Warn("partition not ready — non-terminal rows remain",
			zap.String("partition_name", name),
			zap.Int64("non_terminal_count", nonTerminal),
		)
		return false, nil
	}
	return true, nil
}

// exportAndDrop writes the detached table to cold storage and then reclaims its
// disk. The drop only happens once the export is safely on disk.
func (a *Archiver) exportAndDrop(ctx context.Context, name, ident string) (rowCount int64, path string, err error) {
	path = archivePath(a.cfg.ArchiveDir, name)
	rowCount, err = exportPartition(ctx, a.pool, ident, path)
	if err != nil {
		a.lg.Error("partition export failed",
			zap.String("partition_name", name), zap.String("archive_path", path), zap.Error(err))
		return 0, path, err
	}
	if _, err := a.pool.Exec(ctx, fmt.Sprintf("DROP TABLE %s", ident)); err != nil {
		a.lg.Error("partition drop failed", zap.String("partition_name", name), zap.Error(err))
		return 0, path, err
	}
	return rowCount, path, nil
}

// finishDetached completes the archival of tables that were detached but never
// dropped, which is what a crash between those two steps leaves behind. Such a
// table is invisible to listPartitions (it has no parent), so without this sweep
// its rows are stranded — never exported, or exported but never reclaimed.
//
// Re-exporting a table that was already exported is harmless: same path, same
// content.
func (a *Archiver) finishDetached(ctx context.Context) error {
	names, err := a.listDetachedPartitions(ctx)
	if err != nil {
		a.lg.Error("list detached partitions failed", zap.Error(err))
		return err
	}
	for _, name := range names {
		rowCount, path, err := a.exportAndDrop(ctx, name, pgx.Identifier{name}.Sanitize())
		if err != nil {
			continue // already logged; the next sweep retries
		}
		a.lg.Warn("recovered orphaned partition left detached by an interrupted run",
			zap.String("partition_name", name),
			zap.Int64("row_count", rowCount),
			zap.String("archive_path", path),
		)
	}
	return nil
}

// exportPartition streams the (now detached) table to a gzip CSV via COPY TO
// STDOUT, so the file is written on this process's filesystem rather than the
// Postgres server's. Returns the number of data rows exported.
func exportPartition(ctx context.Context, pool *pgxpool.Pool, ident, path string) (int64, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	sql := fmt.Sprintf("COPY %s TO STDOUT WITH (FORMAT csv, HEADER true)", ident)
	tag, err := conn.Conn().PgConn().CopyTo(ctx, gz, sql)
	if err != nil {
		_ = gz.Close()
		return 0, fmt.Errorf("copy to stdout: %w", err)
	}
	if err := gz.Close(); err != nil {
		return 0, fmt.Errorf("flush gzip: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("sync %s: %w", path, err)
	}
	return tag.RowsAffected(), nil
}

// listPartitions returns the names of every partition attached to
// scheduled_emails. pg_inherits is a system catalog sqlc cannot model, so this
// is a hand-written query rather than generated.
func (a *Archiver) listPartitions(ctx context.Context) ([]string, error) {
	return scanNames(ctx, a.pool,
		`SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = $1::regclass ORDER BY 1`,
		parentTable)
}

// listDetachedPartitions returns tables that follow the partition naming
// convention but are no longer attached to the parent.
func (a *Archiver) listDetachedPartitions(ctx context.Context) ([]string, error) {
	return scanNames(ctx, a.pool, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema()
		  AND c.relkind = 'r'
		  AND c.relname ~ '^scheduled_emails_y[0-9]{4}m[0-9]{2}$'
		  AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid)
		ORDER BY 1`)
}

// countPartitions returns how many partitions are currently attached to
// scheduled_emails — the value backing hatch_db_active_partitions.
func (a *Archiver) countPartitions(ctx context.Context) (int, error) {
	var n int
	err := a.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass`, parentTable).Scan(&n)
	return n, err
}

// scanNames runs a query returning a single text column and collects it.
func scanNames(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]string, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
