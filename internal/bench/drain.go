package bench

import (
	"context"
	"fmt"
	"time"
)

// StatusCounts is the run's schedules by status. It is the harness's ground
// truth: read from the rows themselves, it cannot be thrown off by a missed
// scrape or a counter reset.
type StatusCounts struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Retrying   int `json:"retrying"`
	Delivered  int `json:"delivered"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

// Total is every schedule the run created.
func (s StatusCounts) Total() int { return s.InFlight() + s.Finished() }

// Finished is the schedules that will not change again.
func (s StatusCounts) Finished() int { return s.Delivered + s.Failed + s.Cancelled }

// InFlight is the schedules the pipeline still owes an outcome.
func (s StatusCounts) InFlight() int { return s.Pending + s.Processing + s.Retrying }

func (s StatusCounts) String() string {
	return fmt.Sprintf("pending=%d processing=%d retrying=%d delivered=%d failed=%d cancelled=%d",
		s.Pending, s.Processing, s.Retrying, s.Delivered, s.Failed, s.Cancelled)
}

// The run's schedules all come due within a day of its start, so its queries
// name that range and Postgres scans only the partitions it covers, instead of
// loading the database it is measuring with scans of all of them.
const runSpan = 24 * time.Hour

func (r *Runner) counts(ctx context.Context) (StatusCounts, error) {
	rows, err := r.DB.Query(ctx, `
		SELECT status::text, count(*) FROM scheduled_emails
		WHERE client_id = $1 AND deliver_at >= $2 AND deliver_at < $3
		GROUP BY status`,
		r.client[:], r.started, r.started.Add(runSpan))
	if err != nil {
		return StatusCounts{}, err
	}
	defer rows.Close()
	var c StatusCounts
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return StatusCounts{}, err
		}
		switch status {
		case "pending":
			c.Pending = n
		case "processing":
			c.Processing = n
		case "retrying":
			c.Retrying = n
		case "delivered":
			c.Delivered = n
		case "failed":
			c.Failed = n
		case "cancelled":
			c.Cancelled = n
		}
	}
	return c, rows.Err()
}

// drainSummary is how long the pipeline took to finish the run's schedules,
// and how fast the workers went while they did.
type drainSummary struct {
	Waited          string  `json:"waited"`
	TimedOut        bool    `json:"timed_out"`
	DeliveredPerSec float64 `json:"delivered_per_sec"`
	WorkerWindow    string  `json:"worker_window"`
}

// awaitDrain waits for every schedule the run created to finish, or for
// DrainTimeout, and records how it went.
func (res *Result) awaitDrain(ctx context.Context, r *Runner) error {
	progress("waiting for %d schedules to finish…", res.Load.Created)
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var counts StatusCounts
	last := ""
	for {
		var err error
		if counts, err = r.counts(ctx); err != nil {
			return err
		}
		if line := counts.String(); line != last {
			progress("  [%5s] %s", time.Since(start).Round(time.Second), line)
			last = line
		}
		// Connection pressure belongs to the busy period: once the run is over
		// the pools are idle, so sample it now.
		var inUse, limit int
		if err := r.DB.QueryRow(ctx, `SELECT count(*), current_setting('max_connections')::int FROM pg_stat_activity`).Scan(&inUse, &limit); err == nil {
			r.peakConns, r.maxConns = max(r.peakConns, inUse), limit
		}
		if counts.Finished() >= res.Load.Created || time.Since(start) > r.cfg.DrainTimeout {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	res.Counts = &counts
	summary := &drainSummary{Waited: time.Since(start).Round(time.Millisecond).String()}
	if counts.Finished() < res.Load.Created {
		summary.TimedOut = true
		res.Warnings = append(res.Warnings, fmt.Sprintf("drain timed out after %s with %d schedule(s) in flight", summary.Waited, counts.InFlight()))
	}

	// The workers' rate comes from the span between the first and last row to
	// finish: the rows carry exact timestamps, where a Prometheus rate over a
	// short window is smeared across scrape intervals.
	var first, final *time.Time // NULL when nothing finished
	var n int
	err := r.DB.QueryRow(ctx, `
		SELECT min(updated_at), max(updated_at), count(*) FROM scheduled_emails
		WHERE client_id = $1 AND deliver_at >= $2 AND deliver_at < $3
		  AND status IN ('delivered', 'failed', 'cancelled')`,
		r.client[:], r.started, r.started.Add(runSpan)).Scan(&first, &final, &n)
	if err != nil {
		return fmt.Errorf("delivery window: %w", err)
	}
	if n > 1 && final.After(*first) {
		span := final.Sub(*first)
		summary.DeliveredPerSec = float64(n) / span.Seconds()
		summary.WorkerWindow = span.Round(time.Millisecond).String()

		// When schedules come due over a span, the workers cannot finish them
		// in less, so a fast pipeline reports Count/spread: the load's shape,
		// not its capacity. That invalidates a ceiling measurement; e2e spreads
		// its load on purpose and is judged on lateness instead.
		if spread := r.cfg.Spread; res.Scenario == "delivery" && spread > 0 && span < 2*spread {
			res.Warnings = append(res.Warnings, fmt.Sprintf(
				"THROUGHPUT NOT VALID: the %s worker window is within 2x the %s deliver_at spread, "+
					"so the rate is bounded by how fast schedules came due, not by the workers. "+
					"Run with BENCH_SPREAD=0 to measure the ceiling.",
				summary.WorkerWindow, spread))
		}
	}
	if r.peakConns > 0 {
		res.Metrics["postgres_connections_peak"] = float64(r.peakConns)
		res.Metrics["postgres_connections_max"] = float64(r.maxConns)
	}
	res.Drain = summary
	return nil
}
