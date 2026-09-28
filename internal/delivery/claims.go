package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

// claims make sure each attempt at a schedule is sent by one worker at most. A
// worker claims (schedule, attempt) in Redis before sending, and confirms it
// once the provider has accepted the email, so a duplicate record for the same
// attempt can tell a send in flight from one that is done.
type claims struct {
	redis rueidis.Client
}

type claimState int

const (
	claimAcquired claimState = iota // this worker owns the send
	claimInFlight                   // another worker owns it and has not confirmed it
	claimSent                       // another worker sent it and confirmed so
)

const (
	// claimTTL is how long an unconfirmed claim holds off other workers. It has
	// to outlast a send, and it has to lapse before reconciliation re-enqueues
	// a row stuck in processing (after ten minutes): a worker that dies
	// mid-send must not keep its schedule from ever being retried.
	claimTTL = 5 * time.Minute
	// sentTTL is how long a confirmed send keeps duplicates from resending it.
	sentTTL = 7 * 24 * time.Hour
)

func claimKey(scheduleID uuid.UUID, attempt int16) string {
	return fmt.Sprintf("idempotency:%s:%d", scheduleID, attempt)
}

// claim tries to take the send of one attempt, and when another worker already
// has, reports how far that worker got.
func (c *claims) claim(ctx context.Context, scheduleID uuid.UUID, attempt int16) (claimState, error) {
	key := claimKey(scheduleID, attempt)
	err := c.redis.Do(ctx, c.redis.B().Set().Key(key).Value("claimed").Nx().Ex(claimTTL).Build()).Error()
	if err == nil {
		return claimAcquired, nil
	}
	if !rueidis.IsRedisNil(err) {
		return 0, err
	}
	// The key exists. If it expired since, the attempt is nobody's; the next
	// redelivery or reconciliation will pick it up.
	holder, err := c.redis.Do(ctx, c.redis.B().Get().Key(key).Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return 0, err
	}
	if holder == "sent" {
		return claimSent, nil
	}
	return claimInFlight, nil
}

// confirmSent records that an attempt's email went out.
func (c *claims) confirmSent(ctx context.Context, scheduleID uuid.UUID, attempt int16) error {
	return c.redis.Do(ctx, c.redis.B().Set().Key(claimKey(scheduleID, attempt)).Value("sent").Ex(sentTTL).Build()).Error()
}
