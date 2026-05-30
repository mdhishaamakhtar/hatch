package delivery

import (
	"context"
	"time"
)

// RunRouterTicker is G3: refill every provider's leaky bucket on a fixed cadence
// for the lifetime of the pod. It is the only writer of bucket tokens; G2 is the
// only consumer. Both go through the Router's RWMutex.
func RunRouterTicker(ctx context.Context, r *Router, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Refill()
		}
	}
}
