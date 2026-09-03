package verify

import (
	"fmt"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
)

// Config is loaded once at boot via pkg/config.Load[Config]. Each Hatch service
// declares its own Config so env coupling is explicit.
//
// Connection-critical values (the admin key, DSNs, brokers) come from the
// hatch-secrets Secret via envFrom and are required — `notEmpty` alongside
// `required` because a Secret can supply a key with a blank value. The query
// endpoints default to the well-known ClusterDNS names and stay overridable.
type Config struct {
	AdminKey     string `env:"ADMIN_API_KEY,required,notEmpty"`
	DatabaseURL  string `env:"DATABASE_URL,required,notEmpty"`
	RedisAddr    string `env:"REDIS_ADDR,required,notEmpty"`
	KafkaBrokers string `env:"KAFKA_BROKERS,required,notEmpty"`

	APIBase  string `env:"VERIFY_API_URL"   envDefault:"http://api.hatch.svc.cluster.local:9021"`
	PromURL  string `env:"VERIFY_PROM_URL"  envDefault:"http://observability-kps-prometheus.observability.svc.cluster.local:9090"`
	LokiURL  string `env:"LOKI_ENDPOINT"    envDefault:"http://observability-loki-gateway.observability.svc.cluster.local"`
	TempoURL string `env:"VERIFY_TEMPO_URL" envDefault:"http://observability-tempo.observability.svc.cluster.local:3200"`

	SchedReplicas int    `env:"VERIFY_SCHEDULER_REPLICAS" envDefault:"2"`
	SchedPort     int    `env:"SCHEDULER_ADMIN_PORT"      envDefault:"9022"`
	SchedDomain   string `env:"VERIFY_SCHEDULER_DOMAIN"   envDefault:"scheduler.hatch.svc.cluster.local"`

	// ScheduleLeadSeconds is how far ahead batch schedules are posted — just
	// past the API's minimum now→deliver_at horizon so they fire within the run.
	ScheduleLeadSeconds int `env:"VERIFY_SCHEDULE_LEAD_SECONDS" envDefault:"150"`

	// Resend real-send check. The audit always exercises a live Resend send to
	// the sandbox recipient; the key must be present in hatch-secrets.
	ResendAPIKey string `env:"VERIFY_RESEND_API_KEY"`
	ResendFrom   string `env:"VERIFY_RESEND_FROM" envDefault:"verify@nexia.hishaam.dev"`
	ResendTo     string `env:"VERIFY_RESEND_TO"   envDefault:"delivered@resend.dev"`

	// RetryFailRecipient is the MockProvider fail sentinel — must match the
	// delivery worker's MOCK_PROVIDER_FAIL_RECIPIENT. A send to it always fails
	// transiently, letting the retry check drive a row through all three tiers.
	RetryFailRecipient string `env:"VERIFY_RETRY_FAIL_RECIPIENT" envDefault:"fail@mock.test"`
}

// Brokers splits KafkaBrokers into a slice of broker addresses.
func (c Config) Brokers() []string { return kafka.ParseBrokers(c.KafkaBrokers) }

// SchedulerURL returns the per-pod admin URL for scheduler ordinal i, reached
// over the StatefulSet's headless service DNS.
func (c Config) SchedulerURL(i int) string {
	return fmt.Sprintf("http://scheduler-%d.%s:%d", i, c.SchedDomain, c.SchedPort)
}
