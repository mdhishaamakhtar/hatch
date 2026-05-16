package config

import (
	"testing"
)

type sample struct {
	Name string `env:"PROBE_NAME" envDefault:"hatch"`
	Port int    `env:"PROBE_PORT" envDefault:"7777"`
}

func TestLoad_defaults(t *testing.T) {
	got, err := Load[sample]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "hatch" {
		t.Errorf("Name = %q, want hatch", got.Name)
	}
	if got.Port != 7777 {
		t.Errorf("Port = %d, want 7777", got.Port)
	}
}

func TestLoad_envOverride(t *testing.T) {
	t.Setenv("PROBE_NAME", "override")
	t.Setenv("PROBE_PORT", "9000")
	got, err := Load[sample]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "override" {
		t.Errorf("Name = %q, want override", got.Name)
	}
	if got.Port != 9000 {
		t.Errorf("Port = %d, want 9000", got.Port)
	}
}
