package bench

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// Scenario is one benchmark. Each answers a single question about a single
// stage, because the three stages differ by orders of magnitude and a blended
// number would only ever report the slowest one.
type Scenario struct {
	Name     string
	Question string
	Run      func(context.Context, *Runner) (*Result, error)
}

// Scenarios is the registry the CLI dispatches on.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name:     "ingest",
			Question: "How many schedules per second can the API accept, and what limits it?",
			Run:      runIngest,
		},
		{
			Name:     "delivery",
			Question: "How many emails per second can the delivery workers send?",
			Run:      runDelivery,
		},
		{
			Name:     "e2e",
			Question: "Under a sustained load the API can actually accept, does the pipeline hold the latency SLA?",
			Run:      runE2E,
		},
	}
}

// Runner holds everything a scenario needs. One is built per invocation.
type Runner struct {
	cfg    Config
	api    *apiClient
	prom   *promClient
	pool   *pgxpool.Pool
	http   *http.Client
	client benchClient
	obs    *observer

	// Opts are the CLI knobs a scenario reads.
	Opts Options
}

// Options are the per-run knobs. Defaults are set by the CLI.
type Options struct {
	Count   int
	Workers int
	RPS     float64
	Label   string

	// Spread distributes deliver_at across this span. Zero means every schedule
	// matures at the same instant, which is what a stage-ceiling measurement
	// needs: any spread puts a floor of Count/Spread on the observed rate, so a
	// fast pipeline ends up measuring the load shape instead of itself.
	Spread time.Duration
}

// NewRunner connects every dependency and provisions the benchmark's own
// client. The caller must Close it.
func NewRunner(ctx context.Context, cfg Config, opts Options) (*Runner, error) {
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres (is `make port-forward` running?): %w", err)
	}

	api := newAPIClient(cfg.APIBase, 64)

	// Preflight before anything expensive. A scheduler port-forward that died
	// with the last rollout is invisible until forceAllPolls runs, which is
	// after the load phase — so the run burns minutes of work and then fails on
	// a connection refused. Check every dependency while failing is still free.
	if err := preflight(ctx, cfg, api); err != nil {
		pool.Close()
		return nil, err
	}

	name := "bench-" + time.Now().UTC().Format("20060102-150405")
	// max_rps far above any rate this stack can serve: the per-client limiter is
	// not under test here, and the schema default of 100 would silently cap an
	// ingest ceiling measurement.
	client, err := api.provisionClient(ctx, cfg.AdminKey, name, 1_000_000)
	if err != nil {
		pool.Close()
		return nil, err
	}
	obs, err := newObserver(pool, client.ID)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return &Runner{
		cfg:    cfg,
		api:    api,
		prom:   newPromClient(cfg.PromURL),
		pool:   pool,
		http:   &http.Client{Timeout: 15 * time.Second},
		client: client,
		obs:    obs,
		Opts:   opts,
	}, nil
}

// Close soft-deletes the benchmark client and releases the pool. The schedules
// stay in Postgres for inspection.
func (r *Runner) Close(ctx context.Context) {
	if err := r.api.deleteClient(ctx, r.cfg.AdminKey, r.client.ID); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not delete benchmark client %s: %v\n", r.client.ID, err)
	}
	r.pool.Close()
}

// forceAllPolls nudges every scheduler pod to poll now.
//
// Without this a run would have to wait out SCHEDULER_POLL_INTERVAL (1h) before
// the wheel saw its rows. Both pods are polled because the keyspace is hash-
// sharded across them — whichever pod owns a given schedule must be the one
// that loads it.
func (r *Runner) forceAllPolls(ctx context.Context) error {
	for _, u := range r.cfg.SchedulerAdminURLs {
		if err := forcePoll(ctx, r.http, u, r.cfg.AdminKey); err != nil {
			return fmt.Errorf("force poll: %w", err)
		}
	}
	return nil
}

// runIngest measures the API's schedule-create ceiling.
//
// deliver_at is placed far enough out that nothing matures during the run: this
// scenario is about the ingest path only, and letting the wheel fire mid-run
// would put delivery work on the same node and contaminate the measurement.
//
// Load is offered unthrottled so the server, not the harness, sets the pace.
func runIngest(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("ingest", r)

	load := runLoad(ctx, r.api, r.client.APIKey, loadSpec{
		Count:     r.Opts.Count,
		Workers:   r.Opts.Workers,
		TargetRPS: r.Opts.RPS,
		DeliverAt: time.Now().Add(30 * time.Minute),
	})
	res.Load = &load
	res.checkLoadHealth()
	res.finish()

	if err := res.collectAPIMetrics(ctx, r); err != nil {
		res.Warnings = append(res.Warnings, err.Error())
	}
	return res, nil
}

// runDelivery measures how fast the workers drain and send.
//
// The schedules are packed into a short deliver_at spread so the wheel hands
// the whole set to Kafka almost at once; from there the workers are the only
// thing pacing the run. Throughput is then computed from the first and last
// terminal row's timestamps — the workers' own wall clock, independent of how
// long the load phase took.
func runDelivery(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("delivery", r)

	deliverAt := time.Now().Add(r.cfg.ScheduleLead).Truncate(time.Second)
	load := runLoad(ctx, r.api, r.client.APIKey, loadSpec{
		Count:        r.Opts.Count,
		Workers:      r.Opts.Workers,
		TargetRPS:    r.Opts.RPS,
		DeliverAt:    deliverAt,
		SpreadAcross: r.Opts.Spread,
	})
	res.Load = &load
	res.checkLoadHealth()
	if err := errNothingCreated(load); err != nil {
		return nil, err
	}

	if err := r.forceAllPolls(ctx); err != nil {
		return nil, err
	}
	res.Note("forced an out-of-band poll on %d scheduler pod(s)", len(r.cfg.SchedulerAdminURLs))
	res.Note("deliver_at spread: %s", spreadLabel(r.Opts.Spread))

	if err := res.awaitDrain(ctx, r, load.Created); err != nil {
		return nil, err
	}
	res.finish()

	if err := res.collectDeliveryMetrics(ctx, r); err != nil {
		res.Warnings = append(res.Warnings, err.Error())
	}
	return res, nil
}

// runE2E is the integrated run: a sustained rate the API can actually serve,
// spread across wheel slots the way real traffic would be, carried all the way
// to a terminal state and judged against the latency SLA.
func runE2E(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("e2e", r)

	load := runLoad(ctx, r.api, r.client.APIKey, loadSpec{
		Count:        r.Opts.Count,
		Workers:      r.Opts.Workers,
		TargetRPS:    r.Opts.RPS,
		Relative:     true,
		Lead:         r.cfg.ScheduleLead,
		SpreadAcross: r.Opts.Spread,
	})
	res.Load = &load
	res.checkLoadHealth()
	if err := errNothingCreated(load); err != nil {
		return nil, err
	}

	if err := r.forceAllPolls(ctx); err != nil {
		return nil, err
	}
	if err := res.awaitDrain(ctx, r, load.Created); err != nil {
		return nil, err
	}
	res.finish()

	if err := res.collectDeliveryMetrics(ctx, r); err != nil {
		res.Warnings = append(res.Warnings, err.Error())
	}
	if err := res.collectAPIMetrics(ctx, r); err != nil {
		res.Warnings = append(res.Warnings, err.Error())
	}
	res.judgeSLA()
	return res, nil
}

// errNothingCreated turns an empty load phase into an error that says why.
// The status breakdown matters: a run that created nothing because every
// request was a 400 is a misconfigured benchmark, while one that got 429s is a
// rate limit and one that got transport errors is a connectivity problem — and
// they need completely different fixes.
func errNothingCreated(load LoadResult) error {
	if load.Created > 0 {
		return nil
	}
	detail := fmt.Sprintf("attempted=%d errors=%d rate_limited=%d", load.Attempted, load.Errors, load.RateLimited)
	for code, n := range load.OtherStatus {
		detail += fmt.Sprintf(" http_%d=%d", code, n)
	}
	if load.OtherStatus[400] > 0 {
		detail += "\n  a 400 here is usually deliver_at outside the API horizon:" +
			" BENCH_SCHEDULE_LEAD must exceed API_MIN_SCHEDULE_HORIZON"
	}
	return fmt.Errorf("no schedules were created; nothing to measure (%s)", detail)
}

// spreadLabel renders the maturity spread for the report.
func spreadLabel(d time.Duration) string {
	if d == 0 {
		return "none (all schedules mature in a single wheel slot)"
	}
	return d.String()
}

// preflight verifies every endpoint the run will need, before it provisions
// anything or generates load. Each failure names the fix, because every one of
// them is a missing port-forward or a stopped stack rather than a real result.
func preflight(ctx context.Context, cfg Config, api *apiClient) error {
	hc := &http.Client{Timeout: 5 * time.Second}

	if resp, err := api.do(ctx, http.MethodGet, "/healthz", "", nil); err != nil || resp.code != http.StatusOK {
		return fmt.Errorf("scheduler-api not reachable at %s (is the stack up? `make up-all`): %v", cfg.APIBase, err)
	}
	for _, u := range cfg.SchedulerAdminURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u+"/healthz", nil)
		if err != nil {
			return err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return fmt.Errorf("scheduler admin %s not reachable (run `make bench-pf`; "+
				"per-pod forwards break whenever the scheduler pods are replaced): %w", u, err)
		}
		_ = resp.Body.Close()
	}
	if _, _, err := newPromClient(cfg.PromURL).scalar(ctx, "up"); err != nil {
		return fmt.Errorf("prometheus not reachable at %s (run `make bench-pf`): %w", cfg.PromURL, err)
	}
	return nil
}
