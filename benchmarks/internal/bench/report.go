package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// SLA is the latency contract the e2e scenario is judged against, stated as
// deliver_at → delivered.
var SLA = struct{ P50, P95, P99 time.Duration }{
	P50: 500 * time.Millisecond,
	P95: 2 * time.Second,
	P99: 30 * time.Second,
}

// Result is one scenario's full record: what was run, what happened, and enough
// environment detail to make the numbers reproducible.
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
	Verdict  []Verdict          `json:"verdict,omitempty"`
	Notes    []string           `json:"notes,omitempty"`
	Warnings []string           `json:"warnings,omitempty"`

	window time.Duration // run span, used for the Prometheus lookback
}

// drainSummary is how long the pipeline took to settle and what it settled to.
type drainSummary struct {
	Waited          string  `json:"waited"`
	TimedOut        bool    `json:"timed_out"`
	DeliveredPerSec float64 `json:"delivered_per_sec"`
	WorkerWindow    string  `json:"worker_window"`
}

// Verdict is one SLA line.
type Verdict struct {
	Name   string `json:"name"`
	Target string `json:"target"`
	Actual string `json:"actual"`
	Pass   bool   `json:"pass"`
}

func newResult(scenario string, r *Runner) *Result {
	return &Result{
		Scenario:  scenario,
		Label:     r.Opts.Label,
		StartedAt: time.Now(),
		GitCommit: gitCommit(),
		Replicas:  replicaCounts(),
		ClientID:  r.client.ID,
		Metrics:   map[string]float64{},
	}
}

func (res *Result) finish() {
	res.EndedAt = time.Now()
	res.window = res.EndedAt.Sub(res.StartedAt)
	res.Duration = res.window.Round(time.Millisecond).String()
}

// Note records a human-readable fact about how the run was conducted.
func (res *Result) Note(format string, args ...any) {
	res.Notes = append(res.Notes, fmt.Sprintf(format, args...))
}

// awaitDrain waits for the pipeline to settle and records how it went.
func (res *Result) awaitDrain(ctx context.Context, r *Runner, expect int) error {
	fmt.Printf("  waiting for %d schedules to reach a terminal state…\n", expect)
	last := ""
	state, err := r.obs.waitForDrain(ctx, expect, r.cfg.DrainTimeout, 2*time.Second, func(s drainState) {
		if line := s.Counts.String(); line != last {
			fmt.Printf("    [%5s] %s\n", s.Waited.Round(time.Second), line)
			last = line
		}
	})
	if err != nil {
		return err
	}

	counts := state.Counts
	res.Counts = &counts
	summary := &drainSummary{
		Waited:   state.Waited.Round(time.Millisecond).String(),
		TimedOut: state.TimedOut,
	}
	if state.TimedOut {
		res.Warnings = append(res.Warnings,
			fmt.Sprintf("drain timed out after %s with %d row(s) still in flight",
				summary.Waited, counts.InFlight()))
	}

	// Throughput from the rows' own timestamps: the span between the first and
	// last terminal write is the workers' actual working window, which excludes
	// however long the load phase and the wait for maturity took.
	first, lastT, n, err := r.obs.deliveryWindow(ctx)
	if err != nil {
		return err
	}
	if span := lastT.Sub(first); span > 0 && n > 1 {
		summary.DeliveredPerSec = float64(n) / span.Seconds()
		summary.WorkerWindow = span.Round(time.Millisecond).String()

		// A throughput number is only the pipeline's if the pipeline was what
		// paced it. When schedules mature over a span, the observed window can
		// never be shorter than that span, so a fast pipeline silently reports
		// Count/spread — its load shape, not its capacity. Refuse to let that
		// pass as a result.
		//
		// Only the stage-ceiling scenario is held to this. e2e deliberately
		// spreads its load to imitate real arrivals and is judged on latency, so
		// a spread-bounded window there is the intended shape, not a mistake.
		if spread := r.Opts.Spread; res.Scenario == "delivery" && spread > 0 && span < 2*spread {
			res.Warnings = append(res.Warnings, fmt.Sprintf(
				"THROUGHPUT NOT VALID: the %s worker window is within 2x the %s deliver_at spread, "+
					"so this rate is bounded by how fast schedules matured, not by the workers. "+
					"Re-run with --spread 0 to measure the stage ceiling.",
				summary.WorkerWindow, spread))
		}
	}
	res.Drain = summary
	return nil
}

// collectDeliveryMetrics reads the delivery-side numbers out of Prometheus.
func (res *Result) collectDeliveryMetrics(ctx context.Context, r *Runner) error {
	window := res.window + 30*time.Second // cover the scrape that closed the run

	q, err := r.prom.e2eQuantiles(ctx, window)
	if err != nil {
		return fmt.Errorf("e2e quantiles: %w", err)
	}
	if q.Present {
		res.E2E = &q
	} else {
		res.Warnings = append(res.Warnings,
			"no e2e latency observations in the window — the histogram was not scraped or nothing was delivered")
	}

	for name, metric := range map[string]string{
		"sends_total":     "hatch_delivery_sends_total",
		"retries_total":   "hatch_delivery_retries_total",
		"failed_total":    "hatch_delivery_failed_total",
		"idempotency_ops": "hatch_delivery_idempotency_total",
	} {
		if v, ok, err := r.prom.counterIncrease(ctx, metric, window); err == nil && ok {
			res.Metrics[name] = v
		}
	}
	if v, ok, err := r.prom.histogramQuantile(ctx, "hatch_delivery_provider_send_duration_seconds", 0.95, window); err == nil && ok {
		res.Metrics["provider_send_p95_seconds"] = v
	}
	if v, ok, err := r.prom.histogramQuantile(ctx, "hatch_delivery_batch_duration_seconds", 0.95, window); err == nil && ok {
		res.Metrics["batch_duration_p95_seconds"] = v
	}
	return nil
}

// checkLoadHealth surfaces load that never landed. A run with a healthy
// Created count can still be missing a chunk of its intended load, and a
// throughput or latency figure drawn from a partial, silently-truncated
// population is not the figure it claims to be.
func (res *Result) checkLoadHealth() {
	l := res.Load
	if l == nil {
		return
	}
	if n := l.OtherStatus[400]; n > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"%d of %d creates were rejected 400 — most often deliver_at falling inside "+
				"API_MIN_SCHEDULE_HORIZON because the load phase ran long. "+
				"Only the %d accepted schedules are represented below.",
			n, l.Attempted, l.Created))
	}
	if l.Errors > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%d transport errors during load", l.Errors))
	}
	if l.RateLimited > 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"%d creates were rate limited — the per-client max_rps bound this run, not the server", l.RateLimited))
	}
}

// collectAPIMetrics reads the server's own view of the ingest path, which is
// worth having next to the client-side numbers: a gap between them is queueing
// the handler's histogram cannot see.
func (res *Result) collectAPIMetrics(ctx context.Context, r *Runner) error {
	window := res.window + 30*time.Second
	if v, ok, err := r.prom.histogramQuantile(ctx, "hatch_api_request_duration_seconds", 0.95, window); err == nil && ok {
		res.Metrics["api_request_p95_seconds"] = v
	}
	if v, ok, err := r.prom.counterIncrease(ctx, "hatch_api_requests_total", window); err == nil && ok {
		res.Metrics["api_requests_total"] = v
	}
	if v, ok, err := r.prom.counterIncrease(ctx, "hatch_api_rate_limited_total", window); err == nil && ok {
		res.Metrics["api_rate_limited_total"] = v
	}
	return nil
}

// judgeSLA turns the measured quantiles into pass/fail lines.
func (res *Result) judgeSLA() {
	if res.E2E == nil {
		res.Verdict = append(res.Verdict, Verdict{
			Name: "e2e latency", Target: "measured", Actual: "no data", Pass: false,
		})
		return
	}
	for _, c := range []struct {
		name   string
		target time.Duration
		actual float64
	}{
		{"e2e p50", SLA.P50, res.E2E.P50},
		{"e2e p95", SLA.P95, res.E2E.P95},
		{"e2e p99", SLA.P99, res.E2E.P99},
	} {
		actual := time.Duration(c.actual * float64(time.Second))
		res.Verdict = append(res.Verdict, Verdict{
			Name:   c.name,
			Target: "≤ " + c.target.String(),
			Actual: actual.Round(time.Millisecond).String(),
			Pass:   actual <= c.target,
		})
	}
	if res.Counts != nil {
		res.Verdict = append(res.Verdict, Verdict{
			Name:   "no stranded rows",
			Target: "0 in flight",
			Actual: fmt.Sprintf("%d", res.Counts.InFlight()),
			Pass:   res.Counts.InFlight() == 0,
		})
	}
}

// Passed reports whether every verdict line passed.
func (res *Result) Passed() bool {
	for _, v := range res.Verdict {
		if !v.Pass {
			return false
		}
	}
	return true
}

// Write renders the result as markdown and json under dir, and returns the
// markdown path.
func (res *Result) Write(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	stamp := res.StartedAt.UTC().Format("20060102-150405")
	base := filepath.Join(dir, fmt.Sprintf("%s-%s", res.Scenario, stamp))

	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(base+".json", raw, 0o644); err != nil {
		return "", err
	}
	md := base + ".md"
	if err := os.WriteFile(md, []byte(res.Markdown()), 0o644); err != nil {
		return "", err
	}
	return md, nil
}

// Markdown renders the human-readable report.
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
		p("| Created (2xx) | %d |", l.Created)
		p("| Rate limited (429) | %d |", l.RateLimited)
		p("| Transport errors | %d |", l.Errors)
		for code, n := range l.OtherStatus {
			p("| HTTP %d | %d |", code, n)
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
		p("| Time to settle | %s |", d.Waited)
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
		p("\n## End-to-end latency (deliver_at → delivered)\n")
		p("| Quantile | Value |")
		p("|---|---|")
		p("| p50 | %s |", time.Duration(q.P50*float64(time.Second)).Round(time.Millisecond))
		p("| p95 | %s |", time.Duration(q.P95*float64(time.Second)).Round(time.Millisecond))
		p("| p99 | %s |", time.Duration(q.P99*float64(time.Second)).Round(time.Millisecond))
	}

	if len(res.Metrics) > 0 {
		p("\n## Prometheus\n")
		p("| Metric | Value |")
		p("|---|---|")
		for _, k := range sortedKeys(res.Metrics) {
			p("| %s | %.3f |", k, res.Metrics[k])
		}
	}

	if len(res.Verdict) > 0 {
		p("\n## Verdict\n")
		p("| Check | Target | Actual | |")
		p("|---|---|---|---|")
		for _, v := range res.Verdict {
			mark := "FAIL"
			if v.Pass {
				mark = "PASS"
			}
			p("| %s | %s | %s | %s |", v.Name, v.Target, v.Actual, mark)
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

func sortedKeys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// gitCommit records which build produced the numbers. Unknown is not fatal —
// a benchmark run outside a checkout is still a valid run.
func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// replicaCounts reads the deployed replica counts, which are the single most
// important piece of context for a throughput number.
func replicaCounts() map[string]int {
	out := map[string]int{}
	for _, kind := range []struct{ res, name string }{
		{"deployment", "api"},
		{"statefulset", "scheduler"},
		{"deployment", "delivery-worker"},
		{"deployment", "retry-consumer"},
	} {
		raw, err := exec.Command("kubectl", "-n", "hatch", "get", kind.res, kind.name,
			"-o", "jsonpath={.status.readyReplicas}").Output()
		if err != nil {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d", &n); err == nil {
			out[kind.name] = n
		}
	}
	return out
}
