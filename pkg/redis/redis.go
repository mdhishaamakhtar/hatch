// Package redis wraps the rueidis client used for the per-client provider
// cache and the per-send idempotency lock.
package redis

import (
	"fmt"

	"github.com/redis/rueidis"
)

// NewClient dials Redis. addr is a host:port pair (e.g. "redis:6379").
// rueidis enables auto-pipelining by default.
func NewClient(addr string) (rueidis.Client, error) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
	})
	if err != nil {
		return nil, fmt.Errorf("rueidis: %w", err)
	}
	return c, nil
}
