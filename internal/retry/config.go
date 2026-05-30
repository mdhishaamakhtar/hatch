package retry

import (
	"strings"
	"time"
)

// Config is loaded once at boot via pkg/config.Load[Config]. The retry consumer
// is stateless, so it needs only Kafka, tracing, and the drain cadence per tier.
//
// The interval defaults are the production tiers from the HLD (1m/5m/30m); the
// dev cluster overrides them to a few seconds via the Helm chart so `make
// verify` and demos don't wait minutes for a retry to flow through.
type Config struct {
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`

	OTLPEndpoint string `env:"OTLP_ENDPOINT"`

	AdminPort int `env:"RETRY_ADMIN_PORT" envDefault:"9024"`

	// ConsumerGroupPrefix is suffixed with the tier name to form each tier's
	// durable consumer group (e.g. "retry-consumer-1min").
	ConsumerGroupPrefix string `env:"RETRY_CONSUMER_GROUP_PREFIX" envDefault:"retry-consumer"`

	// DrainBatchSize bounds how many records one PollRecords call returns per
	// drain iteration; FetchMaxWait bounds how long a single poll blocks before
	// the tier is considered drained for this cycle.
	DrainBatchSize int           `env:"RETRY_DRAIN_BATCH"     envDefault:"1000"`
	FetchMaxWait   time.Duration `env:"RETRY_FETCH_MAX_WAIT"  envDefault:"1s"`

	// Per-tier drain cadence. A message's effective delay is bounded by its
	// tier interval — coarse by design.
	Interval1Min  time.Duration `env:"RETRY_INTERVAL_1MIN"  envDefault:"1m"`
	Interval5Min  time.Duration `env:"RETRY_INTERVAL_5MIN"  envDefault:"5m"`
	Interval30Min time.Duration `env:"RETRY_INTERVAL_30MIN" envDefault:"30m"`

	ShutdownTimeoutMS int `env:"RETRY_SHUTDOWN_MS" envDefault:"10000"`
}

// Brokers splits KafkaBrokers into a slice of broker addresses.
func (c Config) Brokers() []string {
	parts := strings.Split(c.KafkaBrokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Tiers returns the three tier consumers derived from config, in ascending
// delay order. The group name is the configured prefix plus the tier label.
func (c Config) Tiers() []Tier {
	return []Tier{
		{Name: "1min", Topic: TopicRetry1Min, Group: c.ConsumerGroupPrefix + "-1min", Interval: c.Interval1Min},
		{Name: "5min", Topic: TopicRetry5Min, Group: c.ConsumerGroupPrefix + "-5min", Interval: c.Interval5Min},
		{Name: "30min", Topic: TopicRetry30Min, Group: c.ConsumerGroupPrefix + "-30min", Interval: c.Interval30Min},
	}
}
