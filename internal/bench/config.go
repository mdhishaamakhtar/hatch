// Package bench is the Hatch benchmark harness. It provisions a throwaway
// client, drives load through the API, waits for the pipeline to settle, and
// reports what each stage sustained.
//
// It runs as a one-shot Job inside the cluster, reaching Postgres, Prometheus,
// the API and each scheduler pod over ClusterDNS — the same shape as
// internal/verify, and for the same reason. A benchmark run lasts tens of
// minutes, and a host port-forward that drops halfway through costs the whole
// run; nothing in the measurement path should depend on one.
//
// The host side (scripts/bench.sh) only does what needs a control-plane client:
// scaling replicas between sweep points and collecting each Job's JSON result.
package bench

import (
	"fmt"
	"strings"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
)

// Config is loaded once at boot via pkg/config.Load[Config], the same mechanism
// every Hatch service uses. Defaults are the in-cluster addresses; the
// connection-critical values come from the hatch-secrets Secret via envFrom.
type Config struct {
	// APIBase is reached through the API's ClusterIP rather than the
	// LoadBalancer, so the measurement never leaves the cluster network.
	APIBase string `env:"BENCH_API_URL" envDefault:"http://api.hatch.svc.cluster.local:9021"`

	AdminKey    string `env:"ADMIN_API_KEY,required,notEmpty"`
	DatabaseURL string `env:"DATABASE_URL,required,notEmpty"`

	// KafkaBrokers is unused by the scenarios today but is required in the
	// Secret, and asserting it here keeps a misconfigured Job failing at boot
	// rather than midway through a long run.
	KafkaBrokers string `env:"KAFKA_BROKERS,required,notEmpty"`

	PromURL string `env:"BENCH_PROM_URL" envDefault:"http://observability-kps-prometheus.observability.svc.cluster.local:9090"`

	// Scheduler pods are addressed individually through the StatefulSet's
	// headless service: the keyspace is hash-sharded across them, so a forced
	// poll has to reach every pod, not one behind a load balancer.
	SchedReplicas int    `env:"BENCH_SCHEDULER_REPLICAS" envDefault:"2"`
	SchedPort     int    `env:"SCHEDULER_ADMIN_PORT"     envDefault:"9022"`
	SchedDomain   string `env:"BENCH_SCHEDULER_DOMAIN"   envDefault:"scheduler.hatch.svc.cluster.local"`

	// ScheduleLead is how far ahead of now deliver_at is placed. It must clear
	// the API's API_MIN_SCHEDULE_HORIZON with enough margin for the load phase
	// itself to finish before the earliest schedule matures.
	ScheduleLead time.Duration `env:"BENCH_SCHEDULE_LEAD" envDefault:"2m30s"`

	// MetricsSettle is how long to wait after the pipeline drains before reading
	// Prometheus. Scrapes are every 30s, so a query issued the instant the last
	// email lands sees a window that does not contain it yet and reports "no
	// data" for a run that in fact succeeded. Two scrape intervals plus margin.
	MetricsSettle time.Duration `env:"BENCH_METRICS_SETTLE" envDefault:"70s"`

	// DrainTimeout bounds the wait for every posted schedule to reach a terminal
	// state. A run that exceeds it reports what it reached rather than hanging.
	DrainTimeout time.Duration `env:"BENCH_DRAIN_TIMEOUT" envDefault:"20m"`

	// Scenario and its knobs. The Job passes these as env rather than flags so
	// one Job manifest serves every sweep point.
	Scenario string        `env:"BENCH_SCENARIO"    envDefault:"e2e"`
	Count    int           `env:"BENCH_COUNT"       envDefault:"400"`
	Workers  int           `env:"BENCH_WORKERS"     envDefault:"32"`
	RPS      float64       `env:"BENCH_RPS"         envDefault:"0"`
	Spread   time.Duration `env:"BENCH_SPREAD"      envDefault:"0s"`
	Label    string        `env:"BENCH_LABEL"`

	// Run metadata supplied by the host orchestrator. The Job runs from a
	// distroless image with no git and no kubectl, so it is told what build and
	// what topology it is measuring rather than trying to discover it.
	GitCommit string `env:"BENCH_GIT_COMMIT" envDefault:"unknown"`
	Replicas  string `env:"BENCH_REPLICAS"`
}

// ReplicaCounts parses the "name=n,name=n" summary the host passes in.
func (c Config) ReplicaCounts() map[string]int {
	out := map[string]int{}
	for _, pair := range strings.Split(c.Replicas, ",") {
		name, count, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(count, "%d", &n); err == nil {
			out[name] = n
		}
	}
	return out
}

// SchedulerURLs returns the per-pod admin URLs for the configured replica count.
func (c Config) SchedulerURLs() []string {
	out := make([]string, 0, c.SchedReplicas)
	for i := range c.SchedReplicas {
		out = append(out, fmt.Sprintf("http://scheduler-%d.%s:%d", i, c.SchedDomain, c.SchedPort))
	}
	return out
}

// Brokers splits KafkaBrokers into a slice of broker addresses.
func (c Config) Brokers() []string { return kafka.ParseBrokers(c.KafkaBrokers) }
