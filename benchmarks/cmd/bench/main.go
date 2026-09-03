// bench runs one Hatch benchmark scenario against a running local cluster and
// writes a markdown + json report.
//
// It expects `make up-all` to have completed and `make bench-pf` to be holding
// the port-forwards it reads through (Postgres, Prometheus, and each scheduler
// pod's admin API).
//
//	go run ./benchmarks/cmd/bench --scenario ingest --count 200
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mdhishaamakhtar/hatch/benchmarks/internal/bench"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark failed:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		name    = flag.String("scenario", "", "scenario to run: "+strings.Join(scenarioNames(), ", "))
		count   = flag.Int("count", 200, "schedules to create")
		workers = flag.Int("workers", 16, "concurrent load generators")
		rps     = flag.Float64("rps", 0, "offered rate cap; 0 means unthrottled")
		label   = flag.String("label", "", "free-text label recorded in the report")
		spread  = flag.Duration("spread", 0, "spread deliver_at across this span; 0 matures everything in one wheel slot")
		list    = flag.Bool("list", false, "list the scenarios and exit")
	)
	flag.Parse()

	if *list || *name == "" {
		for _, s := range bench.Scenarios() {
			fmt.Printf("  %-10s %s\n", s.Name, s.Question)
		}
		if *name == "" && !*list {
			return fmt.Errorf("--scenario is required")
		}
		return nil
	}

	var scenario *bench.Scenario
	for _, s := range bench.Scenarios() {
		if s.Name == *name {
			scenario = &s
			break
		}
	}
	if scenario == nil {
		return fmt.Errorf("unknown scenario %q (see --list)", *name)
	}

	cfg, err := config.Load[bench.Config]()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("== %s ==\n%s\n\n", scenario.Name, scenario.Question)

	runner, err := bench.NewRunner(ctx, cfg, bench.Options{
		Count: *count, Workers: *workers, RPS: *rps, Label: *label, Spread: *spread,
	})
	if err != nil {
		return err
	}
	// The client is torn down on a fresh context: ctx may already be cancelled
	// by Ctrl-C, and cleanup should still run.
	defer runner.Close(context.WithoutCancel(ctx))

	res, err := scenario.Run(ctx, runner)
	if err != nil {
		return err
	}

	path, err := res.Write(cfg.ReportDir)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println(res.Markdown())
	fmt.Println("report:", path)

	if len(res.Verdict) > 0 && !res.Passed() {
		return fmt.Errorf("SLA not met")
	}
	return nil
}

func scenarioNames() []string {
	out := make([]string, 0, len(bench.Scenarios()))
	for _, s := range bench.Scenarios() {
		out = append(out, s.Name)
	}
	return out
}
