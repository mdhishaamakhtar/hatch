// Command bench runs one benchmark scenario against a deployed Hatch stack;
// see internal/bench. The report goes to stderr. Stdout carries only the
// result, as JSON between markers, for scripts/bench.sh to collect from the
// Job's log.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/bench"
)

const (
	resultBegin = "---BENCH-RESULT-BEGIN---"
	resultEnd   = "---BENCH-RESULT-END---"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark failed:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[bench.Config]()
	if err != nil {
		return err
	}
	scenario, ok := bench.Scenarios[cfg.Scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q (have %s)", cfg.Scenario, strings.Join(slices.Sorted(maps.Keys(bench.Scenarios)), ", "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "== %s ==\n%s\n\n", cfg.Scenario, scenario.Question)
	runner, err := bench.NewRunner(ctx, cfg)
	if err != nil {
		return err
	}
	// Not ctx, which a SIGTERM cancels: the client should be deleted anyway.
	defer runner.Close(context.WithoutCancel(ctx))

	res, err := scenario.Run(ctx, runner)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, res.Markdown())

	out, err := json.Marshal(res)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n%s\n%s\n", resultBegin, out, resultEnd)
	return nil
}
