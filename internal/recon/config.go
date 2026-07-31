package recon

import (
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
)

// Config is loaded once at boot via pkg/config.Load[Config]. The reconciliation
// cron needs Postgres (to find stuck rows), Kafka (to re-enqueue them), and the
// sweep cadence.
//
// Interval defaults to the production 24h from the Build Plan; the dev cluster
// overrides it to a few seconds via the Helm chart so `make verify` and demos
// see a sweep promptly.
type Config struct {
	DatabaseURL  string `env:"DATABASE_URL,required"`
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`

	OTLPEndpoint string `env:"OTLP_ENDPOINT"`

	AdminPort int `env:"RECON_ADMIN_PORT" envDefault:"9025"`

	// Interval between reconciliation sweeps.
	Interval time.Duration `env:"RECON_INTERVAL" envDefault:"24h"`

	// RunOnStart runs one sweep immediately at boot (before the first tick) so the
	// last-run gauge and recovery counters appear without waiting a full interval.
	RunOnStart bool `env:"RECON_RUN_ON_START" envDefault:"true"`

	ShutdownTimeout time.Duration `env:"RECON_SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

// Brokers splits KafkaBrokers into a slice of broker addresses.
func (c Config) Brokers() []string { return kafka.ParseBrokers(c.KafkaBrokers) }
