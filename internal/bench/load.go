package bench

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"
)

// LoadResult is what a load phase achieved. Only created schedules count
// toward the achieved rate: a run that was rate limited or erroring still sent
// at the target rate.
type LoadResult struct {
	Attempted   int            `json:"attempted"`
	Created     int            `json:"created"`
	RateLimited int            `json:"rate_limited"`
	Errors      int            `json:"transport_errors"`
	OtherStatus map[int]int    `json:"other_status,omitempty"`
	Duration    time.Duration  `json:"duration_ns"`
	AchievedRPS float64        `json:"achieved_rps"`
	Latency     LatencySummary `json:"latency"`
}

// LatencySummary is request latency as the harness saw it. Set beside the
// API's own histogram, a gap between the two is queueing outside the handler.
type LatencySummary struct {
	P50 time.Duration `json:"p50_ns"`
	P95 time.Duration `json:"p95_ns"`
	P99 time.Duration `json:"p99_ns"`
	Max time.Duration `json:"max_ns"`
}

// loadSpec says when a load phase's schedules come due: all at DeliverAt, or
// each Lead after it is created, and either way spread over Spread.
//
// A fixed DeliverAt puts a whole ceiling run in one wheel slot, but it draws
// nearer while the load runs, so a long load's last requests fall inside the
// API's minimum horizon and are rejected. Anything modelling real arrivals
// uses Lead.
type loadSpec struct {
	DeliverAt time.Time
	Lead      time.Duration
	Spread    time.Duration
}

// load creates cfg.Count schedules from cfg.Workers concurrent workers, at no
// more than cfg.RPS when that is set.
func (r *Runner) load(ctx context.Context, spec loadSpec) *LoadResult {
	type outcome struct {
		code    int
		err     error
		latency time.Duration
	}
	// Each request has a slot of its own, so the workers never contend on a
	// lock, which at these rates would measure the harness instead of the API.
	outcomes := make([]outcome, r.cfg.Count)

	seqs := make(chan int)
	go func() {
		defer close(seqs)
		var tick <-chan time.Time
		if r.cfg.RPS > 0 {
			t := time.NewTicker(time.Duration(float64(time.Second) / r.cfg.RPS))
			defer t.Stop()
			tick = t.C
		}
		for i := range r.cfg.Count {
			if tick != nil {
				select {
				case <-tick:
				case <-ctx.Done():
					return
				}
			}
			select {
			case seqs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for range r.cfg.Workers {
		wg.Go(func() {
			for seq := range seqs {
				deliverAt := spec.DeliverAt
				if spec.Lead > 0 {
					deliverAt = time.Now().Add(spec.Lead)
				}
				if spec.Spread > 0 {
					// Consecutive schedules come due in consecutive wheel slots.
					deliverAt = deliverAt.Add(time.Duration(seq) * time.Second % spec.Spread)
				}
				sent := time.Now()
				resp, err := r.API(ctx, http.MethodPost, "/v1/schedules", r.key, map[string]any{
					"deliver_at":      deliverAt.UnixMilli(),
					"recipient_email": fmt.Sprintf("bench+%d@mock.test", seq),
					"from_email":      "bench@hatch.test",
					"subject":         "hatch benchmark",
					"body":            "benchmark payload",
				})
				outcomes[seq] = outcome{code: resp.Code, err: err, latency: time.Since(sent)}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := &LoadResult{Attempted: r.cfg.Count, OtherStatus: map[int]int{}, Duration: elapsed}
	var latencies []time.Duration
	for _, o := range outcomes {
		switch {
		case o.err != nil:
			res.Errors++
		case o.code == http.StatusCreated:
			res.Created++
			latencies = append(latencies, o.latency)
		case o.code == http.StatusTooManyRequests:
			res.RateLimited++
		case o.code == 0:
			res.Attempted-- // never sent: the run was cancelled first
		default:
			res.OtherStatus[o.code]++
		}
	}
	res.AchievedRPS = float64(res.Created) / elapsed.Seconds()
	res.Latency = summarize(latencies)
	return res
}

// summarize takes nearest-rank percentiles: with samples this few,
// interpolating would invent precision.
func summarize(ds []time.Duration) LatencySummary {
	if len(ds) == 0 {
		return LatencySummary{}
	}
	slices.Sort(ds)
	at := func(q float64) time.Duration { return ds[min(int(q*float64(len(ds))), len(ds)-1)] }
	return LatencySummary{P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: ds[len(ds)-1]}
}
