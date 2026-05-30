// Package delivery holds the Phase 3 delivery-worker service: a 3-goroutine
// pipeline that consumes `emails.due` from Kafka, hydrates each schedule from
// Postgres, routes the send through a provider (mock or Resend) behind a
// per-(client,vendor) circuit breaker + leaky bucket, and drives the
// scheduled_emails status machine to a terminal state. See the LLD §Delivery
// Workers for the full design.
package delivery

import (
	"strings"
	"time"

	"github.com/mdhishaamakhtar/hatch/pkg/provider"
)

// Config is loaded once at boot via pkg/config.Load[Config]. Each Hatch service
// declares its own Config so env coupling is explicit.
type Config struct {
	DatabaseURL  string `env:"DATABASE_URL,required"`
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`
	RedisAddr    string `env:"REDIS_ADDR,required"`

	// ProviderCredKey is the base64 Tink keyset used to decrypt per-client
	// provider credentials (the API encrypts them with the same key).
	ProviderCredKey string `env:"PROVIDER_CRED_KEY,required"`

	OTLPEndpoint string `env:"OTLP_ENDPOINT"`

	AdminAPIKey string `env:"ADMIN_API_KEY,required"`
	AdminPort   int    `env:"DELIVERY_ADMIN_PORT" envDefault:"9023"`

	ConsumerGroup string `env:"DELIVERY_CONSUMER_GROUP" envDefault:"delivery-workers"`
	BatchSize     int    `env:"DELIVERY_BATCH_SIZE"     envDefault:"1000"`

	ClientCacheTTL time.Duration `env:"DELIVERY_CLIENT_CACHE_TTL" envDefault:"5m"`
	IdempotencyTTL time.Duration `env:"DELIVERY_IDEMPOTENCY_TTL" envDefault:"168h"`

	// ProviderTick is G3's leaky-bucket refill cadence. ProviderRatePerSec is
	// the steady-state send rate per (client, vendor); the bucket capacity is the
	// same value (≈1s of burst).
	ProviderTick       time.Duration `env:"DELIVERY_PROVIDER_TICK"         envDefault:"100ms"`
	ProviderRatePerSec int           `env:"DELIVERY_PROVIDER_RATE_PER_SEC" envDefault:"1000"`

	MaxRetries int `env:"DELIVERY_MAX_RETRIES" envDefault:"3"`

	// Circuit breaker tuning (per client+vendor).
	BreakerMinRequests  uint32        `env:"DELIVERY_BREAKER_MIN_REQUESTS"  envDefault:"20"`
	BreakerFailureRatio float64       `env:"DELIVERY_BREAKER_FAILURE_RATIO" envDefault:"0.5"`
	BreakerOpenTimeout  time.Duration `env:"DELIVERY_BREAKER_OPEN_TIMEOUT"  envDefault:"30s"`

	ShutdownTimeoutMS int `env:"DELIVERY_SHUTDOWN_MS" envDefault:"10000"`

	// Mock provider tuning (MOCK_PROVIDER_* env). Parsed into the nested struct.
	Mock provider.MockConfig
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

// RefillPerTick is how many tokens each leaky bucket gains per G3 tick, derived
// from the steady-state rate and the tick cadence. Always at least 1.
func (c Config) RefillPerTick() int {
	n := int(float64(c.ProviderRatePerSec) * c.ProviderTick.Seconds())
	if n < 1 {
		return 1
	}
	return n
}
