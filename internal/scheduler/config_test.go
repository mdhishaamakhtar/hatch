package scheduler

import (
	"testing"

	"github.com/mdhishaamakhtar/hatch/pkg/config"
)

// The defaults asserted here are contracts other things depend on: the admin
// port the Helm probes hit, the wheel path the StatefulSet volume mounts, and
// the single-pod sharding assumption a bare `go run` relies on.
func TestConfigDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("KAFKA_BROKERS", "k:9092")
	t.Setenv("ADMIN_API_KEY", "k")

	cfg, err := config.Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PodIndex != 0 || cfg.TotalPods != 1 {
		t.Errorf("sharding defaults = %d/%d, want 0/1 (single pod)", cfg.PodIndex, cfg.TotalPods)
	}
	if cfg.AdminPort != 9022 {
		t.Errorf("AdminPort = %d, want 9022 (matches the Helm probe)", cfg.AdminPort)
	}
	if cfg.WheelDBPath != "/var/lib/hatch/wheel.db" {
		t.Errorf("WheelDBPath = %q, want the StatefulSet volume mount path", cfg.WheelDBPath)
	}
}
