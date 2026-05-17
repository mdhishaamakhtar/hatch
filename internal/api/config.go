package api

// Config holds the env-driven configuration for the scheduler-api service.
// Loaded once at startup via pkg/config.Load.
type Config struct {
	Port              int    `env:"API_PORT"           envDefault:"9021"`
	DatabaseURL       string `env:"DATABASE_URL,required"`
	RedisAddr         string `env:"REDIS_ADDR,required"`
	OTLPEndpoint      string `env:"OTLP_ENDPOINT"`
	AdminAPIKey       string `env:"ADMIN_API_KEY,required"`
	ProviderCredKey   string `env:"PROVIDER_CRED_KEY,required"`
	BcryptCost        int    `env:"BCRYPT_COST"        envDefault:"12"`
	MaxBodyBytes      int64  `env:"API_MAX_BODY_BYTES" envDefault:"65536"`
	ShutdownTimeoutMS int    `env:"API_SHUTDOWN_MS"    envDefault:"10000"`
}
