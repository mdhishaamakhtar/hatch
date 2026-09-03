package bench

import (
	"context"
	"sort"
	"sync"
	"time"
)

// LoadResult is what one load phase actually achieved. Attempted and the status
// breakdown are reported separately from Achieved RPS because a run that gets
// rate-limited or errors out still "sends" at the target rate — only counting
// 2xx responses keeps the throughput number honest.
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

// LatencySummary is the client-observed request latency. This is deliberately
// measured on the harness side as well as in the server's own histogram: if the
// two disagree, the gap is queueing outside the handler, which a server-side
// metric cannot see.
type LatencySummary struct {
	P50 time.Duration `json:"p50_ns"`
	P95 time.Duration `json:"p95_ns"`
	P99 time.Duration `json:"p99_ns"`
	Max time.Duration `json:"max_ns"`
}

// loadSpec describes one load phase.
type loadSpec struct {
	// Count is how many schedules to create.
	Count int
	// Workers is the concurrency offered to the API. Past the point where the
	// server saturates, more workers only add queueing — throughput flattens
	// while latency climbs, so sweeping this is how the ceiling is confirmed.
	Workers int
	// TargetRPS caps the offered rate. Zero means unthrottled — send as fast as
	// the workers can, which is how a ceiling is found.
	TargetRPS float64
	// DeliverAt is the maturity time given to every schedule. A single instant
	// concentrates the whole run into one wheel slot; SpreadAcross widens it.
	// Ignored when Relative is set.
	DeliverAt time.Time

	// Relative recomputes deliver_at as (now + Lead) at the moment each request
	// is sent, instead of anchoring every schedule to one instant fixed before
	// the load began.
	//
	// A fixed anchor silently breaks whenever the load phase runs long: the
	// anchor keeps approaching while requests are still going out, so the tail of
	// the run lands inside API_MIN_SCHEDULE_HORIZON and is rejected 400. Any run
	// whose load phase approaches the schedule lead hits this. Ceiling runs still
	// want the fixed anchor (one shared wheel slot); anything modelling real
	// arrivals wants this.
	Relative bool
	Lead     time.Duration
	// SpreadAcross distributes deliver_at over this span, round-robin by second,
	// so the wheel fires them across many ticks instead of one.
	SpreadAcross time.Duration
}

// runLoad drives schedule creation and returns what was achieved.
//
// Latency is recorded per request into a preallocated slice indexed by sequence
// number, so the workers never contend on a shared accumulator — at these rates
// a mutex around the recorder would be measuring the harness, not the server.
func runLoad(ctx context.Context, api *apiClient, apiKey string, spec loadSpec) LoadResult {
	type outcome struct {
		code    int
		err     error
		latency time.Duration
	}
	outcomes := make([]outcome, spec.Count)

	// A nil limiter means unthrottled.
	var tick <-chan time.Time
	if spec.TargetRPS > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / spec.TargetRPS))
		defer t.Stop()
		tick = t.C
	}

	seqC := make(chan int)
	go func() {
		defer close(seqC)
		for i := range spec.Count {
			if tick != nil {
				select {
				case <-tick:
				case <-ctx.Done():
					return
				}
			}
			select {
			case seqC <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for range spec.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range seqC {
				deliverAt := spec.DeliverAt
				if spec.Relative {
					deliverAt = time.Now().Add(spec.Lead)
				}
				if spec.SpreadAcross > 0 {
					// Round-robin by whole seconds: consecutive schedules land in
					// consecutive wheel slots.
					offset := time.Duration(seq) * time.Second % spec.SpreadAcross
					deliverAt = deliverAt.Add(offset)
				}
				reqStart := time.Now()
				code, err := api.createSchedule(ctx, apiKey, deliverAt, seq)
				outcomes[seq] = outcome{code: code, err: err, latency: time.Since(reqStart)}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := LoadResult{
		Attempted:   spec.Count,
		OtherStatus: map[int]int{},
		Duration:    elapsed,
	}
	latencies := make([]time.Duration, 0, spec.Count)
	for _, o := range outcomes {
		switch {
		case o.err != nil:
			res.Errors++
		case o.code == 201 || o.code == 200:
			res.Created++
			latencies = append(latencies, o.latency)
		case o.code == 429:
			res.RateLimited++
		case o.code == 0:
			// Never dispatched: the context was cancelled before this sequence
			// number was reached. Not an error against the server.
			res.Attempted--
		default:
			res.OtherStatus[o.code]++
		}
	}
	if elapsed > 0 {
		res.AchievedRPS = float64(res.Created) / elapsed.Seconds()
	}
	res.Latency = summarize(latencies)
	return res
}

// summarize reduces raw latencies to the reported percentiles. Nearest-rank is
// used rather than interpolation — with sample counts this small, interpolating
// invents precision the data does not have.
func summarize(ds []time.Duration) LatencySummary {
	if len(ds) == 0 {
		return LatencySummary{}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	at := func(q float64) time.Duration {
		i := int(q * float64(len(ds)))
		if i >= len(ds) {
			i = len(ds) - 1
		}
		return ds[i]
	}
	return LatencySummary{P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: ds[len(ds)-1]}
}
