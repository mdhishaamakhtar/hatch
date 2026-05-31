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

// ArchiveOnce walks every attached partition of scheduled_emails and, for each
// one whose month is fully in the past, archives it iff all its rows are terminal
// (delivered/failed/cancelled): detach → export to <ArchiveDir>/<name>.csv.gz →
// drop. The pre-created current/future runway is never touched (its months are
// not fully past). Returns how many fully-past partitions were checked and how
// many of those were archived. One partition's failure is logged and skipped; it
// does not abort the sweep.
func ArchiveOnce(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg Config, tr trace.Tracer, lg *zap.Logger) (checked, archived int, err error) {
	ctx, span := tr.Start(ctx, "archival.run")
	defer span.End()
	start := time.Now()

	if err := os.MkdirAll(cfg.ArchiveDir, 0o755); err != nil {
		span.RecordError(err)
		lg.Error("create archive dir failed", zap.String("archive_dir", cfg.ArchiveDir), zap.Error(err))
		return 0, 0, err
	}

	names, err := listPartitions(ctx, pool)
	if err != nil {
		span.RecordError(err)
		lg.Error("list partitions failed", zap.Error(err))
		return 0, 0, err
	}

	now := time.Now()
	for _, name := range names {
		y, m, ok := parsePartitionMonth(name)
		if !ok || !isFullyPast(y, m, now) {
			continue
		}
		checked++
		if done, e := archivePartition(ctx, pool, q, cfg, tr, lg, name); e != nil {
			span.RecordError(e)
			continue
		} else if done {
			archived++
		}
	}

	dur := time.Since(start)
	span.SetAttributes(
		attribute.Int("partitions_checked", checked),
		attribute.Int("partitions_archived", archived),
		attribute.Float64("duration_seconds", dur.Seconds()),
	)
	mArchived.WithLabelValues().Add(float64(archived))
	mRunDuration.WithLabelValues().Observe(dur.Seconds())
	mLastRun.WithLabelValues().Set(float64(time.Now().Unix()))
	if active, e := countPartitions(ctx, pool); e == nil {
		mActivePartitions.WithLabelValues().Set(float64(active))
	}
	lg.Info("archival run completed",
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
func archivePartition(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg Config, tr trace.Tracer, lg *zap.Logger, name string) (bool, error) {
	ctx, cspan := tr.Start(ctx, "archival.partition.check", trace.WithAttributes(attribute.String("partition_name", name)))
	nonTerminal, err := q.CheckPartitionTerminal(ctx, name)
	if err != nil {
		cspan.RecordError(err)
		cspan.End()
		lg.Error("partition terminal check failed", zap.String("partition_name", name), zap.Error(err))
		return false, err
	}
	cspan.SetAttributes(attribute.Int64("non_terminal_count", nonTerminal))
	cspan.End()
	if nonTerminal > 0 {
		lg.Warn("partition not ready — non-terminal rows remain",
			zap.String("partition_name", name),
			zap.Int64("non_terminal_count", nonTerminal),
		)
		return false, nil
	}

	ctx, aspan := tr.Start(ctx, "archival.partition.archive", trace.WithAttributes(attribute.String("partition_name", name)))
	defer aspan.End()

	ident := pgx.Identifier{name}.Sanitize()

	// Detach — instant, no lock on active partitions.
	if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", parentTable, ident)); err != nil {
		aspan.RecordError(err)
		lg.Error("partition detach failed", zap.String("partition_name", name), zap.Error(err))
		return false, err
	}

	// Export the detached table to compressed CSV for cold storage.
	path := archivePath(cfg.ArchiveDir, name)
	rowCount, err := exportPartition(ctx, pool, ident, path)
	if err != nil {
		aspan.RecordError(err)
		lg.Error("partition export failed", zap.String("partition_name", name), zap.String("archive_path", path), zap.Error(err))
		return false, err
	}

	// Drop the detached table — instant, reclaims disk.
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE %s", ident)); err != nil {
		aspan.RecordError(err)
		lg.Error("partition drop failed", zap.String("partition_name", name), zap.Error(err))
		return false, err
	}

	aspan.SetAttributes(attribute.Int64("row_count", rowCount), attribute.String("archive_path", path))
	lg.Info("partition archived",
		zap.String("partition_name", name),
		zap.Int64("row_count", rowCount),
		zap.String("archive_path", path),
	)
	return true, nil
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
func listPartitions(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = $1::regclass ORDER BY 1`,
		parentTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// countPartitions returns how many partitions are currently attached to
// scheduled_emails — the value backing hatch_db_active_partitions.
func countPartitions(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var n int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass`, parentTable).Scan(&n)
	return n, err
}
