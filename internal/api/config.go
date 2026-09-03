package api

import "time"

// Config holds the env-driven configuration for the scheduler-api service.
// Loaded once at startup via pkg/config.Load.
type Config struct {
	Port             int    `env:"API_PORT"           envDefault:"9021"`
	DatabaseURL      string `env:"DATABASE_URL,required"`
	RedisAddr        string `env:"REDIS_ADDR,required"`
	OTLPEndpoint     string `env:"OTLP_ENDPOINT"`
	AdminAPIKey      string `env:"ADMIN_API_KEY,required"`
	ProviderCredKey  string `env:"PROVIDER_CRED_KEY,required"`
	MaxBodyBytes     int64  `env:"API_MAX_BODY_BYTES" envDefault:"65536"`
	APIEnableSwagger bool   `env:"API_ENABLE_SWAGGER" envDefault:"true"`

	ShutdownTimeout time.Duration `env:"API_SHUTDOWN_TIMEOUT" envDefault:"10s"`

	// MinScheduleHorizon is the minimum allowed time between now and deliver_at.
	// Defaults to 1h (production). Set to a shorter value (e.g. 2m) for local
	// dev/verify runs so acceptance tests can fire schedules quickly.
	MinScheduleHorizon time.Duration `env:"API_MIN_SCHEDULE_HORIZON" envDefault:"1h"`

	// MaxScheduleHorizon bounds deliver_at from above. Beyond the pre-created
	// partition runway an INSERT fails deep in Postgres with "no partition of
	// relation"; rejecting early turns that 500 into a 400 the caller can act on.
	MaxScheduleHorizon time.Duration `env:"API_MAX_SCHEDULE_HORIZON" envDefault:"87600h"`
}
