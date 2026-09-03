// Package bench is the Hatch benchmark harness: it provisions a throwaway
// client, drives load through the public API, waits for the pipeline to settle,
// and reports what each stage actually sustained.
//
// It runs on the host rather than in-cluster (unlike internal/verify), because
// the load generator belongs outside the system under test — traffic reaches the
// API through the same LoadBalancer a real client would use. Only the two
// read-only observers need port-forwards: Postgres for ground truth and
// Prometheus for rates and percentiles.
package bench

import (
	"time"
)

// Config is loaded once at boot via pkg/config.Load[Config], the same mechanism
// every Hatch service uses. The defaults are the host-side addresses from the
// README, so an unconfigured run works against a standard `make up-all` cluster.
type Config struct {
	// APIBase is the LoadBalancer address of scheduler-api.
	APIBase string `env:"BENCH_API_URL" envDefault:"http://localhost:9021"`
	// AdminKey provisions and tears down the benchmark client.
	AdminKey string `env:"ADMIN_API_KEY,required,notEmpty"`

	// DatabaseURL is the host DSN reached through `make port-forward`. Postgres
	// is the harness's ground truth: terminal-state counts come from the rows
	// themselves, never from a metric that could be stale or unscraped.
	DatabaseURL string `env:"HOST_DATABASE_URL" envDefault:"postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable"`

	// PromURL needs its own port-forward; scripts/port-forward.sh does not open
	// one, so `make bench-pf` adds it.
	PromURL string `env:"BENCH_PROM_URL" envDefault:"http://localhost:9090"`

	// SchedulerAdminURLs are the per-pod admin endpoints used to force an
	// out-of-band poll. Without this the wheel would not see freshly-created rows
	// until the next hourly tick, which would make every run an hour long.
	SchedulerAdminURLs []string `env:"BENCH_SCHEDULER_URLS" envSeparator:"," envDefault:"http://localhost:9122,http://localhost:9123"`

	// ScheduleLead is how far ahead of now the benchmark's deliver_at values are
	// placed. It must clear the API's API_MIN_SCHEDULE_HORIZON (2m in the local
	// .env) with enough margin for the load phase itself to finish before the
	// earliest schedule matures.
	ScheduleLead time.Duration `env:"BENCH_SCHEDULE_LEAD" envDefault:"3m"`

	// DrainTimeout bounds the wait for every posted schedule to reach a terminal
	// state. A run that exceeds it is reported as a timeout with the counts it
	// reached, not silently truncated.
	DrainTimeout time.Duration `env:"BENCH_DRAIN_TIMEOUT" envDefault:"20m"`

	// ReportDir is where the markdown + json artifacts land.
	ReportDir string `env:"BENCH_REPORT_DIR" envDefault:"benchmarks/reports"`
}
