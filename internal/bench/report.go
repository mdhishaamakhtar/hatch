package bench

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
)

// Result is one run's full record: what ran, what happened, and enough about
// the environment to reproduce it.
type Result struct {
	Scenario  string    `json:"scenario"`
	Label     string    `json:"label,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Duration  string    `json:"duration"`

	GitCommit string         `json:"git_commit"`
	Replicas  map[string]int `json:"replicas"`
	ClientID  string         `json:"client_id"`

	Load   *LoadResult   `json:"load,omitempty"`
	Drain  *drainSummary `json:"drain,omitempty"`
	Counts *StatusCounts `json:"final_counts,omitempty"`

	E2E      *Quantiles         `json:"e2e_latency_seconds,omitempty"`
	Metrics  map[string]float64 `json:"metrics,omitempty"`
	Checks   []Check            `json:"integrity_checks,omitempty"`
	Notes    []string           `json:"notes,omitempty"`
	Warnings []string           `json:"warnings,omitempty"`
}

// Quantiles are how late sends went out, in seconds past deliver_at.
type Quantiles struct {
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
	Present bool    `json:"present"`
}

// A Check asserts the run was valid, so its numbers can be taken at face
// value: every schedule finished, none was lost. There are deliberately no
// performance targets: a number the system "should" hit would be invented.
type Check struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Pass     bool   `json:"pass"`
}

func newResult(scenario string, r *Runner) *Result {
	return &Result{
		Scenario:  scenario,
		Label:     r.cfg.Label,
		StartedAt: time.Now(),
		GitCommit: r.cfg.GitCommit,
		Replicas:  r.cfg.Replicas,
		ClientID:  r.client.String(),
		Metrics:   map[string]float64{},
	}
}

func (res *Result) finish() {
	res.EndedAt = time.Now()
	res.Duration = res.EndedAt.Sub(res.StartedAt).Round(time.Millisecond).String()
}

// Note records a fact about how the run was conducted.
func (res *Result) Note(format string, args ...any) {
	res.Notes = append(res.Notes, fmt.Sprintf(format, args...))
}

// checkLoad warns about load that never landed: a rate or latency drawn from
// part of the intended load is not the figure it claims to be.
func (res *Result) checkLoad() {
	l := res.Load
	if n := l.OtherStatus[400]; n > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"%d of %d creates were rejected with 400, most often because the load ran long enough for deliver_at "+
				"to fall inside API_MIN_SCHEDULE_HORIZON. Only the %d accepted schedules are measured.",
			n, l.Attempted, l.Created))
	}
	if l.Errors > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%d transport errors during the load", l.Errors))
	}
	if l.RateLimited > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"%d creates were rate limited: the client's max_rps bounded this run, not the API", l.RateLimited))
	}
}

// promWindow is how far back a query has to look, from now, to cover the whole
// run.
func (res *Result) promWindow() string {
	return fmt.Sprintf("%ds", int((time.Since(res.StartedAt) + 30*time.Second).Seconds()))
}

// scalar runs a PromQL query for a single value. It reports no value, rather
// than zero, when the query returns nothing or NaN, which is what a quantile
// over a window without observations returns.
func (r *Runner) scalar(ctx context.Context, expr string) (float64, bool) {
	values, err := r.Query(ctx, expr)
	if err != nil || len(values) == 0 || math.IsNaN(values[0]) || math.IsInf(values[0], 0) {
		return 0, false
	}
	return values[0], true
}

func (r *Runner) quantile(ctx context.Context, histogram string, q float64, window string) (float64, bool) {
	return r.scalar(ctx, fmt.Sprintf(`histogram_quantile(%g, sum by (le) (rate(%s_bucket[%s])))`, q, histogram, window))
}

func (r *Runner) increase(ctx context.Context, counter string, window string) (float64, bool) {
	return r.scalar(ctx, fmt.Sprintf(`sum(increase(%s[%s]))`, counter, window))
}

// collectDeliveryMetrics reads the delivery side of the run from Prometheus.
func (res *Result) collectDeliveryMetrics(ctx context.Context, r *Runner) {
	window := res.promWindow()
	var q Quantiles
	q.P50, q.Present = r.quantile(ctx, "hatch_delivery_e2e_latency_seconds", 0.50, window)
	q.P95, _ = r.quantile(ctx, "hatch_delivery_e2e_latency_seconds", 0.95, window)
	q.P99, _ = r.quantile(ctx, "hatch_delivery_e2e_latency_seconds", 0.99, window)
	if q.Present {
		res.E2E = &q
	} else {
		res.Warnings = append(res.Warnings, "no lateness observations in the window: nothing was delivered, or it was not scraped")
	}

	// The scheduler's numbers are recorded beside the workers' because a
	// ceiling run puts every schedule in one wheel slot: if the scheduler
	// cannot fire them all on one tick, the delivery rate is really its rate.
	for name, counter := range map[string]string{
		"scheduler_fired_total": "hatch_scheduler_fired_total",
		"sends_total":           "hatch_delivery_sends_total",
		"retries_total":         "hatch_delivery_retries_total",
		"failed_total":          "hatch_delivery_failed_total",
		"idempotency_ops":       "hatch_delivery_idempotency_total",
	} {
		if v, ok := r.increase(ctx, counter, window); ok {
			res.Metrics[name] = v
		}
	}
	for name, histogram := range map[string]string{
		"provider_send_p95_seconds":     "hatch_delivery_provider_send_duration_seconds",
		"batch_duration_p95_seconds":    "hatch_delivery_batch_duration_seconds",
		"scheduler_produce_p95_seconds": "hatch_scheduler_kafka_produce_duration_seconds",
	} {
		if v, ok := r.quantile(ctx, histogram, 0.95, window); ok {
			res.Metrics[name] = v
		}
	}
}

// collectAPIMetrics reads the API's own view of the load, to set beside the
// harness's.
func (res *Result) collectAPIMetrics(ctx context.Context, r *Runner) {
	window := res.promWindow()
	if v, ok := r.quantile(ctx, "hatch_api_request_duration_seconds", 0.95, window); ok {
		res.Metrics["api_request_p95_seconds"] = v
	}
	if v, ok := r.increase(ctx, "hatch_api_requests_total", window); ok {
		res.Metrics["api_requests_total"] = v
	}
	if v, ok := r.increase(ctx, "hatch_api_rate_limited_total", window); ok {
		res.Metrics["api_rate_limited_total"] = v
	}
}

// checkIntegrity records whether the run was sound. A failed check means the
// numbers cannot be trusted, not that the system was slow.
func (res *Result) checkIntegrity() {
	if res.Counts != nil {
		res.Checks = append(res.Checks,
			Check{
				Name:     "no schedules left in flight",
				Expected: "0",
				Actual:   fmt.Sprint(res.Counts.InFlight()),
				Pass:     res.Counts.InFlight() == 0,
			},
			Check{
				Name:     "every schedule accounted for",
				Expected: fmt.Sprintf("%d rows", res.Load.Created),
				Actual:   fmt.Sprintf("%d rows", res.Counts.Total()),
				Pass:     res.Counts.Total() == res.Load.Created,
			})
	}
	if res.Scenario == "e2e" && res.E2E == nil {
		res.Checks = append(res.Checks, Check{Name: "lateness histogram populated", Expected: "present", Actual: "no data"})
	}
}

// Markdown renders the result as a report.
func (res *Result) Markdown() string {
	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	p("# Hatch benchmark — %s", res.Scenario)
	if res.Label != "" {
		p("\n**%s**", res.Label)
	}
	p("\n| | |")
	p("|---|---|")
	p("| Started | %s |", res.StartedAt.UTC().Format(time.RFC3339))
	p("| Duration | %s |", res.Duration)
	p("| Commit | `%s` |", res.GitCommit)
	for _, name := range []string{"api", "scheduler", "delivery-worker", "retry-consumer"} {
		if n, ok := res.Replicas[name]; ok {
			p("| %s replicas | %d |", name, n)
		}
	}

	if l := res.Load; l != nil {
		p("\n## Ingest (client-observed)\n")
		p("| Metric | Value |")
		p("|---|---|")
		p("| Attempted | %d |", l.Attempted)
		p("| Created (201) | %d |", l.Created)
		p("| Rate limited (429) | %d |", l.RateLimited)
		p("| Transport errors | %d |", l.Errors)
		for _, code := range slices.Sorted(maps.Keys(l.OtherStatus)) {
			p("| HTTP %d | %d |", code, l.OtherStatus[code])
		}
		p("| Wall time | %s |", l.Duration.Round(time.Millisecond))
		p("| **Achieved RPS** | **%.1f** |", l.AchievedRPS)
		p("| Request p50 | %s |", l.Latency.P50.Round(time.Millisecond))
		p("| Request p95 | %s |", l.Latency.P95.Round(time.Millisecond))
		p("| Request p99 | %s |", l.Latency.P99.Round(time.Millisecond))
		p("| Request max | %s |", l.Latency.Max.Round(time.Millisecond))
	}

	if d := res.Drain; d != nil {
		p("\n## Delivery\n")
		p("| Metric | Value |")
		p("|---|---|")
		p("| Time to finish | %s |", d.Waited)
		p("| Timed out | %t |", d.TimedOut)
		if d.WorkerWindow != "" {
			p("| Worker window | %s |", d.WorkerWindow)
			p("| **Delivered/sec** | **%.2f** |", d.DeliveredPerSec)
		}
		if c := res.Counts; c != nil {
			p("| Final states | %s |", c)
		}
	}

	if q := res.E2E; q != nil {
		p("\n## Lateness (deliver_at → delivered)\n")
		p("| Quantile | Value |")
		p("|---|---|")
		p("| p50 | %s |", seconds(q.P50))
		p("| p95 | %s |", seconds(q.P95))
		p("| p99 | %s |", seconds(q.P99))
	}

	if len(res.Metrics) > 0 {
		p("\n## Prometheus\n")
		p("| Metric | Value |")
		p("|---|---|")
		for _, k := range slices.Sorted(maps.Keys(res.Metrics)) {
			p("| %s | %.3f |", k, res.Metrics[k])
		}
	}

	if len(res.Checks) > 0 {
		p("\n## Run integrity\n")
		p("| Check | Expected | Actual | |")
		p("|---|---|---|---|")
		for _, c := range res.Checks {
			mark := "FAIL"
			if c.Pass {
				mark = "PASS"
			}
			p("| %s | %s | %s | %s |", c.Name, c.Expected, c.Actual, mark)
		}
	}

	if len(res.Notes) > 0 {
		p("\n## Notes\n")
		for _, n := range res.Notes {
			p("- %s", n)
		}
	}
	if len(res.Warnings) > 0 {
		p("\n## Warnings\n")
		for _, w := range res.Warnings {
			p("- %s", w)
		}
	}
	return b.String()
}

// seconds renders a number of seconds as a duration.
func seconds(s float64) string {
	return time.Duration(s * float64(time.Second)).Round(time.Millisecond).String()
}
