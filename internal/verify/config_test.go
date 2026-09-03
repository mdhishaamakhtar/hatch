package verify

import (
	"strings"
	"testing"

	"github.com/mdhishaamakhtar/hatch/pkg/config"
)

// setRequired fills in the four connection-critical vars so a test can focus on
// one field at a time.
func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("ADMIN_API_KEY", "admin-key")
	t.Setenv("DATABASE_URL", "postgres://localhost/hatch")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
}

// The verifier runs as a one-shot Job: an incomplete Secret must fail at load
// with every missing key named, not midway through the suite as a stray 401.
func TestLoadNamesEveryMissingRequiredVar(t *testing.T) {
	_, err := config.Load[Config]()
	if err == nil {
		t.Fatal("Load succeeded with no environment set")
	}
	for _, key := range []string{"ADMIN_API_KEY", "DATABASE_URL", "REDIS_ADDR", "KAFKA_BROKERS"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not name %s: %v", key, err)
		}
	}
}

// A Secret can supply a key with a blank value, which `required` alone accepts.
func TestLoadRejectsABlankRequiredVar(t *testing.T) {
	setRequired(t)
	t.Setenv("ADMIN_API_KEY", "")
	if _, err := config.Load[Config](); err == nil {
		t.Fatal("Load accepted a blank ADMIN_API_KEY")
	}
}

// A blank override falls back to the ClusterDNS default rather than pointing
// the verifier at the empty string.
func TestLoadFallsBackToDefaultsOnABlankOverride(t *testing.T) {
	setRequired(t)
	t.Setenv("VERIFY_PROM_URL", "")

	cfg, err := config.Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.HasPrefix(cfg.PromURL, "http://observability-kps-prometheus") {
		t.Errorf("PromURL = %q, want the ClusterDNS default", cfg.PromURL)
	}
	if got, want := cfg.SchedulerURL(1), "http://scheduler-1.scheduler.hatch.svc.cluster.local:9022"; got != want {
		t.Errorf("SchedulerURL(1) = %q, want %q", got, want)
	}
}

func TestBrokersTrimsTheCSV(t *testing.T) {
	setRequired(t)
	t.Setenv("KAFKA_BROKERS", " a:9092 , b:9092 ,")

	cfg, err := config.Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Brokers()
	if len(got) != 2 || got[0] != "a:9092" || got[1] != "b:9092" {
		t.Errorf("Brokers() = %v, want [a:9092 b:9092]", got)
	}
}
