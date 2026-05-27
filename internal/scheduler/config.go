// Package scheduler holds the Phase 2 timer-wheel service: a 3-goroutine
// pipeline (DB poller → wheel builder + bbolt persistence → 1-second ticker
// → Kafka produce) that fires `emails.due` messages at the exact second each
// scheduled email matures. See the LLD §Scheduler for the full design.
package scheduler

import (
	"strings"
	"time"
)

// Config is loaded once at boot via pkg/config.Load[Config]. Each Hatch service
// declares its own Config so env coupling is explicit.
type Config struct {
	PodIndex  int `env:"POD_INDEX"  envDefault:"0"`
	TotalPods int `env:"TOTAL_PODS" envDefault:"1"`

	DatabaseURL string `env:"DATABASE_URL,required"`

	// KafkaBrokers is csv (e.g. "kafka.hatch.svc.cluster.local:9092").
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`

	WheelDBPath string `env:"SCHEDULER_WHEEL_DB_PATH" envDefault:"/var/lib/hatch/wheel.db"`

	AdminAPIKey string `env:"ADMIN_API_KEY,required"`
	AdminPort   int    `env:"SCHEDULER_ADMIN_PORT" envDefault:"9022"`

	OTLPEndpoint string `env:"OTLP_ENDPOINT"`

	ScheduleChannelBuffer int `env:"SCHEDULER_SCHEDULE_CHANNEL_BUFFER" envDefault:"100000"`
	ClearChannelBuffer    int `env:"SCHEDULER_CLEAR_CHANNEL_BUFFER"    envDefault:"64"`

	ShutdownTimeoutMS int `env:"SCHEDULER_SHUTDOWN_MS" envDefault:"10000"`

	// PollInterval is the cadence of G1. Defaults to 1h per LLD.
	// Surfaced as a knob so tests don't need to wait an hour.
	PollInterval time.Duration `env:"SCHEDULER_POLL_INTERVAL" envDefault:"1h"`
}

// Brokers splits KafkaBrokers into a slice of broker addresses.
func (c Config) Brokers() []string {
	parts := strings.Split(c.KafkaBrokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
