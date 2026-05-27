package scheduler

import (
	"reflect"
	"testing"

	"github.com/mdhishaamakhtar/hatch/pkg/config"
)

func TestConfigBrokersCSV(t *testing.T) {
	c := Config{KafkaBrokers: "a:1, b:2 ,c:3"}
	got := c.Brokers()
	want := []string{"a:1", "b:2", "c:3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Brokers() = %v, want %v", got, want)
	}
}

func TestConfigBrokersDropsBlanks(t *testing.T) {
	c := Config{KafkaBrokers: ",,a:1,,"}
	got := c.Brokers()
	if len(got) != 1 || got[0] != "a:1" {
		t.Fatalf("Brokers() = %v", got)
	}
}

func TestConfigLoadDefaults(t *testing.T) {
	// Set the required envs so Load doesn't fail; everything else picks up
	// the envDefault values on the struct.
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("KAFKA_BROKERS", "k:9092")
	t.Setenv("ADMIN_API_KEY", "k")
	cfg, err := config.Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PodIndex != 0 || cfg.TotalPods != 1 {
		t.Errorf("pod_index/total_pods defaults wrong: %d/%d", cfg.PodIndex, cfg.TotalPods)
	}
	if cfg.AdminPort != 9022 {
		t.Errorf("AdminPort default = %d, want 9022", cfg.AdminPort)
	}
	if cfg.WheelDBPath != "/var/lib/hatch/wheel.db" {
		t.Errorf("WheelDBPath default = %q", cfg.WheelDBPath)
	}
	if cfg.ScheduleChannelBuffer != 100000 || cfg.ClearChannelBuffer != 64 {
		t.Errorf("channel buffer defaults wrong: %d/%d", cfg.ScheduleChannelBuffer, cfg.ClearChannelBuffer)
	}
}
