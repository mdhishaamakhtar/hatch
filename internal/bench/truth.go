package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// StatusCounts is the benchmark client's rows grouped by status. This is the
// harness's ground truth: it comes from the rows themselves rather than from a
// metric, so it cannot be wrong because of a missed scrape, a counter reset, or
// an exporter that was never deployed.
type StatusCounts struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Retrying   int `json:"retrying"`
	Delivered  int `json:"delivered"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

// Total is every row the benchmark created.
func (s StatusCounts) Total() int {
	return s.Pending + s.Processing + s.Retrying + s.Delivered + s.Failed + s.Cancelled
}

// Terminal is the rows that will never move again.
func (s StatusCounts) Terminal() int { return s.Delivered + s.Failed + s.Cancelled }

// InFlight is the rows the pipeline still owes an outcome for.
func (s StatusCounts) InFlight() int { return s.Pending + s.Processing + s.Retrying }

func (s StatusCounts) String() string {
	return fmt.Sprintf("pending=%d processing=%d retrying=%d delivered=%d failed=%d cancelled=%d",
		s.Pending, s.Processing, s.Retrying, s.Delivered, s.Failed, s.Cancelled)
}

// observer reads the benchmark's ground truth out of Postgres.
type observer struct {
	pool     *pgxpool.Pool
	clientID []byte

	// Peak Postgres backends seen while the pipeline was busy, and the server's
	// limit, both filled in by waitForDrain.
	peakConns int
	maxConns  int
}

func newObserver(pool *pgxpool.Pool, clientID string) (*observer, error) {
	id, err := uuid.Parse(clientID)
	if err != nil {
		return nil, fmt.Errorf("parse client id: %w", err)
	}
	return &observer{pool: pool, clientID: db.UUIDToBytes(id)}, nil
}

// counts groups the benchmark client's schedules by status in one round trip.
func (o *observer) counts(ctx context.Context) (StatusCounts, error) {
	rows, err := o.pool.Query(ctx,
		`SELECT status::text, count(*) FROM scheduled_emails WHERE client_id = $1 GROUP BY status`,
		o.clientID)
	if err != nil {
		return StatusCounts{}, fmt.Errorf("count by status: %w", err)
	}
	defer rows.Close()

	var out StatusCounts
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return StatusCounts{}, err
		}
		switch status {
		case "pending":
			out.Pending = n
		case "processing":
			out.Processing = n
		case "retrying":
			out.Retrying = n
		case "delivered":
			out.Delivered = n
		case "failed":
			out.Failed = n
		case "cancelled":
			out.Cancelled = n
		}
	}
	return out, rows.Err()
}

// deliveryWindow returns when the benchmark's first and last row reached a
// terminal state, plus the count. Wall-clock throughput is derived from this
// rather than from a Prometheus rate() — the rows carry exact timestamps, while
// a rate over a short window is smeared by the scrape interval.
func (o *observer) deliveryWindow(ctx context.Context) (first, last time.Time, n int, err error) {
	err = o.pool.QueryRow(ctx,
		`SELECT min(updated_at), max(updated_at), count(*)
		   FROM scheduled_emails
		  WHERE client_id = $1 AND status IN ('delivered','failed','cancelled')`,
		o.clientID).Scan(&first, &last, &n)
	if err != nil {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("delivery window: %w", err)
	}
	return first, last, n, nil
}

// drainState is one sample of the drain loop, reported so a run that stalls
// says where it stalled instead of only that it timed out.
type drainState struct {
	Counts   StatusCounts
	Settled  bool
	Waited   time.Duration
	TimedOut bool
}

// connections reports how many Postgres backends are in use and the server's
// limit.
//
// Sampled during the drain rather than after it: connection pressure is a
// property of the busy period, and by the time a run finishes the pools have
// gone idle and the number is meaningless. Each pod runs many sends at once but
// pgx sizes its pool from CPU count, so this is where "concurrency outran the
// connection pool" would become visible instead of being guessed at.
func (o *observer) connections(ctx context.Context) (inUse, max int, err error) {
	if err := o.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity`).Scan(&inUse); err != nil {
		return 0, 0, err
	}
	if err := o.pool.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&max); err != nil {
		return inUse, 0, err
	}
	return inUse, max, nil
}

// waitForDrain polls until every row the benchmark created is terminal, or the
// timeout expires.
//
// It deliberately does not watch Kafka consumer lag: no Kafka exporter is
// deployed in this stack, and "every row reached a terminal state" is the
// stronger claim anyway — lag reaching zero only means the messages were
// consumed, not that the work they represent actually completed.
func (o *observer) waitForDrain(ctx context.Context, expect int, timeout, interval time.Duration, onSample func(drainState)) (drainState, error) {
	o.peakConns, o.maxConns = 0, 0
	start := time.Now()
	deadline := start.Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		counts, err := o.counts(ctx)
		if err != nil {
			return drainState{}, err
		}
		if inUse, max, cerr := o.connections(ctx); cerr == nil {
			if inUse > o.peakConns {
				o.peakConns = inUse
			}
			o.maxConns = max
		}

		state := drainState{Counts: counts, Waited: time.Since(start)}
		state.Settled = counts.Terminal() >= expect
		state.TimedOut = !state.Settled && time.Now().After(deadline)
		if onSample != nil {
			onSample(state)
		}
		if state.Settled || state.TimedOut {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return state, ctx.Err()
		case <-ticker.C:
		}
	}
}
