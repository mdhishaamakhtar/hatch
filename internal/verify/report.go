// Package verify is the in-cluster acceptance auditor for Hatch. It runs as a
// one-shot Job inside the cluster and reaches every dependency over Kubernetes
// ClusterDNS — Postgres, Redis, Kafka, the API, each scheduler pod's per-pod
// headless DNS, and the Prometheus/Loki/Tempo query APIs — so verification
// never depends on host port-forwards.
//
// The suite is cumulative, not per-phase: it asserts everything built so far
// (foundation → API golden path → scheduler → Kafka → observability). New
// phases append checks here rather than spawning a new script.
package verify

import "fmt"

// Reporter accumulates pass/fail outcomes and prints one line per check in the
// same [PASS]/[FAIL] format the old shell scripts used. These lines are a
// report format, not structured logging, so they go straight to stdout.
type Reporter struct {
	fails int
}

// Section prints a header that groups the checks that follow.
func (r *Reporter) Section(name string) { fmt.Printf("\n== %s ==\n", name) }

// Pass records a successful check.
func (r *Reporter) Pass(msg string) { fmt.Printf("  [PASS] %s\n", msg) }

// Passf is Pass with printf-style formatting.
func (r *Reporter) Passf(format string, args ...any) { r.Pass(fmt.Sprintf(format, args...)) }

// Fail records a failed check and bumps the failure count.
func (r *Reporter) Fail(msg string) {
	fmt.Printf("  [FAIL] %s\n", msg)
	r.fails++
}

// Failf is Fail with printf-style formatting.
func (r *Reporter) Failf(format string, args ...any) { r.Fail(fmt.Sprintf(format, args...)) }

// Check records a pass with passMsg when ok, otherwise a fail with failMsg.
func (r *Reporter) Check(ok bool, passMsg, failMsg string) {
	if ok {
		r.Pass(passMsg)
	} else {
		r.Fail(failMsg)
	}
}

// Failures returns how many checks failed so far.
func (r *Reporter) Failures() int { return r.fails }
