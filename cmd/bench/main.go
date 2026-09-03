// bench runs one Hatch benchmark scenario from inside the cluster and prints
// its result.
//
// It is driven entirely by environment variables (BENCH_SCENARIO, BENCH_COUNT,
// …) so that one Job manifest serves every point of a sweep. The human-readable
// report goes to stderr; stdout carries only the result JSON, wrapped in
// markers, so the host orchestrator can collect it from `kubectl logs` without
// parsing prose.
//
//	go run ./benchmarks/cmd/bench      # honours the same env vars locally
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mdhishaamakhtar/hatch/internal/bench"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
)

// Markers delimiting the JSON payload on stdout. The host greps between them,
// which keeps the contract explicit instead of depending on the JSON being the
// only brace-shaped thing in the log.
const (
	jsonBegin = "---BENCH-RESULT-BEGIN---"
	jsonEnd   = "---BENCH-RESULT-END---"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark failed:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load[bench.Config]()
	if err != nil {
		return err
	}

	scenario, err := bench.ScenarioByName(cfg.Scenario)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "== %s ==\n%s\n\n", scenario.Name, scenario.Question)

	runner, err := bench.NewRunner(ctx, cfg, bench.Options{
		Count: cfg.Count, Workers: cfg.Workers, RPS: cfg.RPS,
		Spread: cfg.Spread, Label: cfg.Label,
	})
	if err != nil {
		return err
	}
	// Torn down on a fresh context: ctx may already be cancelled by a SIGTERM,
	// and the throwaway client should still be cleaned up.
	defer runner.Close(context.WithoutCancel(ctx))

	res, err := scenario.Run(ctx, runner)
	if err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, res.Markdown())

	raw, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	fmt.Println(jsonBegin)
	fmt.Println(string(raw))
	fmt.Println(jsonEnd)
	return nil
}
