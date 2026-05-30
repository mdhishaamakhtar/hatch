package verify

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds every endpoint the verifier talks to. Connection-critical
// values (the admin key, DSNs, brokers) come from the hatch-secrets Secret via
// envFrom and are required. The query endpoints default to the well-known
// ClusterDNS names and are overridable for flexibility.
type Config struct {
	AdminKey    string
	DatabaseURL string
	RedisAddr   string
	Brokers     []string

	APIBase  string
	PromURL  string
	LokiURL  string
	TempoURL string

	SchedReplicas int
	SchedPort     int
	SchedDomain   string

	// ScheduleLeadSeconds is how far ahead batch schedules are posted — just
	// past the API's minimum now→deliver_at horizon so they fire within the run.
	ScheduleLeadSeconds int

	// Resend real-send check. The audit always exercises a live Resend send to
	// the sandbox recipient; the key must be present in hatch-secrets.
	ResendAPIKey string
	ResendFrom   string
	ResendTo     string
}

// SchedulerURL returns the per-pod admin URL for scheduler ordinal i, reached
// over the StatefulSet's headless service DNS.
func (c Config) SchedulerURL(i int) string {
	return fmt.Sprintf("http://scheduler-%d.%s:%d", i, c.SchedDomain, c.SchedPort)
}

// LoadConfig reads the verifier configuration from the environment, applying
// ClusterDNS defaults for the observability query endpoints.
func LoadConfig() (Config, error) {
	c := Config{
		AdminKey:    os.Getenv("ADMIN_API_KEY"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisAddr:   os.Getenv("REDIS_ADDR"),

		APIBase:  env("VERIFY_API_URL", "http://api.hatch.svc.cluster.local:9021"),
		PromURL:  env("VERIFY_PROM_URL", "http://observability-kps-prometheus.observability.svc.cluster.local:9090"),
		LokiURL:  env("LOKI_ENDPOINT", "http://observability-loki-gateway.observability.svc.cluster.local"),
		TempoURL: env("VERIFY_TEMPO_URL", "http://observability-tempo.observability.svc.cluster.local:3200"),

		SchedReplicas:       envInt("VERIFY_SCHEDULER_REPLICAS", 2),
		SchedPort:           envInt("SCHEDULER_ADMIN_PORT", 9022),
		SchedDomain:         env("VERIFY_SCHEDULER_DOMAIN", "scheduler.hatch.svc.cluster.local"),
		ScheduleLeadSeconds: envInt("VERIFY_SCHEDULE_LEAD_SECONDS", 150),

		ResendAPIKey: os.Getenv("VERIFY_RESEND_API_KEY"),
		ResendFrom:   env("VERIFY_RESEND_FROM", "verify@nexia.hishaam.dev"),
		ResendTo:     env("VERIFY_RESEND_TO", "delivered@resend.dev"),
	}

	for b := range strings.SplitSeq(env("KAFKA_BROKERS", ""), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}

	var missing []string
	if c.AdminKey == "" {
		missing = append(missing, "ADMIN_API_KEY")
	}
	if c.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if c.RedisAddr == "" {
		missing = append(missing, "REDIS_ADDR")
	}
	if len(c.Brokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
