// Package bench is Hatch's benchmark harness. It runs as a Kubernetes Job
// against a deployed stack: it creates a client of its own, drives load
// through the API, waits for the pipeline to finish what the load started, and
// reports what each stage sustained.
//
// The host side (scripts/bench.sh) only does what needs the control plane:
// scaling replicas between runs and collecting each Job's result.
package bench

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/stack"
)

// Config describes one benchmark run. One Job manifest serves every run of a
// sweep, so the knobs are environment variables rather than flags.
type Config struct {
	stack.Config

	Scenario string  `env:"BENCH_SCENARIO" envDefault:"e2e"`
	Count    int     `env:"BENCH_COUNT"    envDefault:"400"`
	Workers  int     `env:"BENCH_WORKERS"  envDefault:"32"`
	RPS      float64 `env:"BENCH_RPS"      envDefault:"0"` // 0 is unthrottled
	Label    string  `env:"BENCH_LABEL"`

	// Spread distributes deliver_at over this span. Zero puts every schedule in
	// one wheel slot, which is what measuring a stage's ceiling needs: with a
	// spread, the delivery rate can never beat Count/Spread, and a fast
	// pipeline ends up measuring the load's shape instead of itself.
	Spread time.Duration `env:"BENCH_SPREAD" envDefault:"0s"`

	// ScheduleLead is how far ahead deliver_at is placed. It has to clear the
	// API's API_MIN_SCHEDULE_HORIZON, with room for the load to finish first.
	ScheduleLead time.Duration `env:"BENCH_SCHEDULE_LEAD" envDefault:"2m30s"`

	// MetricsSettle is how long to wait after the run before reading
	// Prometheus, so its last increments have been scraped.
	MetricsSettle time.Duration `env:"BENCH_METRICS_SETTLE" envDefault:"70s"`

	// DrainTimeout bounds the wait for every schedule to finish.
	DrainTimeout time.Duration `env:"BENCH_DRAIN_TIMEOUT" envDefault:"20m"`

	// What is being measured, as the host tells it: the Job's image has no git
	// and no kubectl to find out.
	GitCommit string         `env:"BENCH_GIT_COMMIT" envDefault:"unknown"`
	Replicas  map[string]int `env:"BENCH_REPLICAS" envKeyValSeparator:"="` // api=1,scheduler=2,…
}

// A Scenario measures one stage. The stages' ceilings differ by orders of
// magnitude, so a number blending them would only ever report the slowest.
type Scenario struct {
	Question string
	Run      func(context.Context, *Runner) (*Result, error)
}

// Scenarios are the benchmarks, by name.
var Scenarios = map[string]Scenario{
	"ingest":   {"How many schedules per second can the API accept?", runIngest},
	"delivery": {"How many emails per second can the delivery workers send?", runDelivery},
	"e2e":      {"With arrivals spread across wheel slots the way real traffic is, how late do sends run?", runE2E},
}

// A Runner runs scenarios against the stack as a client of its own. Every
// schedule a run creates belongs to that client, which is how the harness
// counts its own rows without touching anyone else's.
type Runner struct {
	*stack.Stack
	cfg     Config
	client  uuid.UUID
	key     string
	started time.Time

	// Postgres connections in use at the busiest moment of the drain, and the
	// server's limit.
	peakConns, maxConns int
}

// NewRunner checks every part of the stack a run needs is reachable, then
// creates the run's client. The caller must Close it.
func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	// The default transport keeps two idle connections per host, which would
	// throttle the load before the API did.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.Workers
	st, err := stack.Connect(ctx, cfg.Config, &http.Client{Transport: transport, Timeout: 30 * time.Second})
	if err != nil {
		return nil, err
	}

	// Fail now rather than after a load phase that took minutes.
	if resp, err := st.API(ctx, http.MethodGet, "/healthz", "", nil); err != nil || resp.Code != http.StatusOK {
		st.Close()
		return nil, fmt.Errorf("API not reachable at %s: %v", cfg.APIURL, err)
	}
	for i := range cfg.SchedulerReplicas {
		if _, err := st.Request(ctx, http.MethodGet, st.Scheduler(i)+"/healthz", "", nil); err != nil {
			st.Close()
			return nil, fmt.Errorf("scheduler %d not reachable (is SCHEDULER_REPLICAS right?): %w", i, err)
		}
	}
	if _, err := st.Query(ctx, "up"); err != nil {
		st.Close()
		return nil, fmt.Errorf("prometheus not reachable at %s: %w", cfg.PromURL, err)
	}

	// max_rps far above anything the stack can serve: the per-client rate
	// limit is not what is being measured.
	name := "bench-" + time.Now().UTC().Format("20060102-150405")
	client, key, err := st.NewClient(ctx, name, 1_000_000, "mock", map[string]string{"api_key": "bench"})
	if err != nil {
		st.Close()
		return nil, err
	}
	return &Runner{Stack: st, cfg: cfg, client: client, key: key, started: time.Now()}, nil
}

// Close deletes the run's client. Its schedules stay in the database, to be
// inspected.
func (r *Runner) Close(ctx context.Context) {
	if err := r.DeleteClient(ctx, r.client); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not delete benchmark client %s: %v\n", r.client, err)
	}
	r.Stack.Close()
}

// runIngest measures how many schedules per second the API accepts. Nothing
// comes due during the run, so no delivery work competes with the API, and
// the load is unthrottled unless BENCH_RPS says otherwise.
func runIngest(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("ingest", r)
	res.Load = r.load(ctx, loadSpec{DeliverAt: time.Now().Add(30 * time.Minute)})
	res.checkLoad()
	res.finish()

	if err := r.settleMetrics(ctx); err != nil {
		return nil, err
	}
	res.collectAPIMetrics(ctx, r)
	return res, nil
}

// runDelivery measures how many emails per second the workers send. Every
// schedule comes due at once (unless BENCH_SPREAD says otherwise), so from
// then on the workers alone set the pace; the rate comes from the rows' own
// timestamps, which leaves out the load phase and the wait for deliver_at.
func runDelivery(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("delivery", r)
	res.Load = r.load(ctx, loadSpec{DeliverAt: time.Now().Add(r.cfg.ScheduleLead).Truncate(time.Second), Spread: r.cfg.Spread})
	res.checkLoad()
	if err := errNothingCreated(res.Load); err != nil {
		return nil, err
	}
	if err := r.Poll(ctx); err != nil {
		return nil, err
	}
	res.Note("polled all %d scheduler shards on demand", r.cfg.SchedulerReplicas)
	res.Note("deliver_at spread: %s", spreadLabel(r.cfg.Spread))
	if err := res.awaitDrain(ctx, r); err != nil {
		return nil, err
	}
	res.finish()

	if err := r.settleMetrics(ctx); err != nil {
		return nil, err
	}
	res.collectDeliveryMetrics(ctx, r)
	res.checkIntegrity()
	return res, nil
}

// runE2E sustains a rate the API can serve, due across wheel slots the way
// real traffic is, and measures how late each email goes out.
func runE2E(ctx context.Context, r *Runner) (*Result, error) {
	res := newResult("e2e", r)
	res.Load = r.load(ctx, loadSpec{Lead: r.cfg.ScheduleLead, Spread: r.cfg.Spread})
	res.checkLoad()
	if err := errNothingCreated(res.Load); err != nil {
		return nil, err
	}
	if err := r.Poll(ctx); err != nil {
		return nil, err
	}
	if err := res.awaitDrain(ctx, r); err != nil {
		return nil, err
	}
	res.finish()

	if err := r.settleMetrics(ctx); err != nil {
		return nil, err
	}
	res.collectDeliveryMetrics(ctx, r)
	res.collectAPIMetrics(ctx, r)
	res.checkIntegrity()
	return res, nil
}

// errNothingCreated explains a load phase that created nothing, which leaves
// nothing to measure. The breakdown says what to fix: 400s are a misconfigured
// run, 429s a rate limit, transport errors a connectivity problem.
func errNothingCreated(load *LoadResult) error {
	if load.Created > 0 {
		return nil
	}
	var detail strings.Builder
	fmt.Fprintf(&detail, "attempted=%d errors=%d rate_limited=%d", load.Attempted, load.Errors, load.RateLimited)
	for code, n := range load.OtherStatus {
		fmt.Fprintf(&detail, " http_%d=%d", code, n)
	}
	if load.OtherStatus[http.StatusBadRequest] > 0 {
		detail.WriteString("\n  a 400 here is usually deliver_at inside the API's horizon:" +
			" BENCH_SCHEDULE_LEAD must exceed API_MIN_SCHEDULE_HORIZON")
	}
	return fmt.Errorf("no schedules were created; nothing to measure (%s)", detail.String())
}

func spreadLabel(d time.Duration) string {
	if d == 0 {
		return "none (every schedule comes due in the same wheel slot)"
	}
	return d.String()
}

// settleMetrics waits for the run's last increments to be scraped. Without it,
// a fast run ends inside one scrape interval and reports no data for work it
// did.
func (r *Runner) settleMetrics(ctx context.Context) error {
	progress("waiting %s for the final Prometheus scrape…", r.cfg.MetricsSettle)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(r.cfg.MetricsSettle):
		return nil
	}
}

// progress reports what a run is doing. It goes to stderr: stdout carries only
// the result.
func progress(format string, args ...any) { fmt.Fprintf(os.Stderr, "  "+format+"\n", args...) }
