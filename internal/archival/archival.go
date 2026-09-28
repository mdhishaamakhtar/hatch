// Package archival is the partition archival cron. scheduled_emails keeps each
// month in its own partition; once a month is over and every schedule in it
// has finished, archival exports the partition to a gzipped CSV and drops it.
package archival

import (
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Config is read from the environment.
type Config struct {
	DatabaseURL string        `env:"DATABASE_URL,required,notEmpty"`
	Port        int           `env:"PORT" envDefault:"9026"`
	Interval    time.Duration `env:"ARCHIVAL_INTERVAL" envDefault:"720h"`
	Dir         string        `env:"ARCHIVE_DIR" envDefault:"/archive"`
}

// partitionLayout parses a partition's name into the month it holds, as named
// by migration 004: scheduled_emails_y2026m05 holds May 2026.
const partitionLayout = "scheduled_emails_y2006m01"

var errUnfinished = errors.New("partition has unfinished schedules")

var (
	tracer = otel.Tracer("archival")

	activePartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hatch_db_active_partitions",
		Help: "Partitions of scheduled_emails, as of the last archival sweep.",
	})
	archivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hatch_archival_partitions_archived_total",
		Help: "Partitions exported and dropped.",
	})
	sweepDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "hatch_archival_run_duration_seconds",
		Help:    "Time to run one archival sweep.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})
	lastSweep = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hatch_archival_last_run_timestamp",
		Help: "Unix time of the last completed sweep. The staleness alert watches it.",
	})
)

// Run sweeps now, then every interval, until ctx is cancelled.
func Run(ctx context.Context, interval time.Duration, lg *zap.Logger, pool *pgxpool.Pool, dir string) {
	service.Every(ctx, interval, func(ctx context.Context) {
		if _, _, err := Sweep(ctx, lg, pool, dir); err != nil {
			lg.Error("archival sweep failed", zap.Error(err))
		}
	})
}

// Sweep archives every partition whose month is over and whose schedules have
// all finished, writing exports to dir. It returns how many past partitions it
// considered and how many it archived. A partition it fails on is logged and
// left for the next sweep.
func Sweep(ctx context.Context, lg *zap.Logger, pool *pgxpool.Pool, dir string) (checked, archived int, err error) {
	ctx, span := tracer.Start(ctx, "archival.sweep")
	defer span.End()
	start := time.Now()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, 0, err
	}
	rows, err := pool.Query(ctx, `SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'scheduled_emails'::regclass`)
	if err != nil {
		return 0, 0, err
	}
	partitions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, 0, err
	}

	for _, name := range partitions {
		if !monthEnded(name, start) {
			continue
		}
		checked++
		path := filepath.Join(dir, name+".csv.gz")
		n, err := archive(ctx, pool, name, path)
		switch {
		case errors.Is(err, errUnfinished):
			lg.Info("partition not archived: it has unfinished schedules", zap.String("partition", name))
		case err != nil:
			span.RecordError(err)
			lg.Error("archive partition", zap.String("partition", name), zap.Error(err))
		default:
			archived++
			lg.Info("partition archived", zap.String("partition", name), zap.Int64("rows", n), zap.String("path", path))
		}
	}

	archivedTotal.Add(float64(archived))
	activePartitions.Set(float64(len(partitions) - archived))
	sweepDuration.Observe(time.Since(start).Seconds())
	lastSweep.SetToCurrentTime()
	span.SetAttributes(attribute.Int("checked", checked), attribute.Int("archived", archived))
	lg.Info("archival sweep completed", zap.Int("checked", checked), zap.Int("archived", archived), zap.Duration("duration", time.Since(start)))
	return checked, archived, nil
}

// monthEnded reports whether name is a partition whose month is over by now.
func monthEnded(name string, now time.Time) bool {
	month, err := time.Parse(partitionLayout, name)
	return err == nil && !month.AddDate(0, 1, 0).After(now)
}

// archive exports a partition to path and drops it, unless a schedule in it is
// still unfinished. The export can be read from the live partition because a
// finished past month never changes again: the API only schedules into the
// future, and no status moves on from delivered, failed or cancelled. A crash
// between the export and the drop just means the next sweep exports it again.
func archive(ctx context.Context, pool *pgxpool.Pool, name, path string) (rows int64, err error) {
	table := pgx.Identifier{name}.Sanitize()
	var unfinished bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM `+table+` WHERE status NOT IN ('delivered', 'failed', 'cancelled'))`,
	).Scan(&unfinished)
	if err != nil {
		return 0, err
	}
	if unfinished {
		return 0, errUnfinished
	}

	if rows, err = export(ctx, pool, table, path); err != nil {
		return 0, err
	}
	_, err = pool.Exec(ctx, `DROP TABLE `+table)
	return rows, err
}

// export streams table to a gzipped CSV at path. COPY TO STDOUT sends the rows
// through this process, so the file lands here rather than on the database
// server's disk.
func export(ctx context.Context, pool *pgxpool.Pool, table, path string) (int64, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tag, err := conn.Conn().PgConn().CopyTo(ctx, gz, `COPY `+table+` TO STDOUT WITH (FORMAT csv, HEADER true)`)
	if err != nil {
		return 0, err
	}
	if err := gz.Close(); err != nil {
		return 0, err
	}
	// The partition is dropped next, so the export must be on disk first.
	return tag.RowsAffected(), f.Sync()
}
