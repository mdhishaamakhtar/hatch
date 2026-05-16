// Package config is a thin generic wrapper around caarlos0/env so every
// service loads its config the same way.
package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

// Load parses environment variables into T using env tags.
// Required fields and defaults are declared on the struct itself.
func Load[T any]() (T, error) {
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}
