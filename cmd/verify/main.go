// verify — Hatch's in-cluster acceptance auditor. Runs as a one-shot Job and
// reaches every dependency over ClusterDNS, so verification never depends on
// host port-forwards. It prints one [PASS]/[FAIL] line per check and exits
// non-zero if any check fails. The suite is cumulative across phases; see
// internal/verify.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mdhishaamakhtar/hatch/internal/verify"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
)

func main() {
	lg, err := logger.New("verify")
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger init failed:", err)
		os.Exit(1)
	}
	defer func() { _ = lg.Sync() }()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	os.Exit(verify.Run(ctx, lg))
}
