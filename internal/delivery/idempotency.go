package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// errIdemUnavailable means the idempotency lock could not be checked because
// Redis was unreachable. The processor leaves the row `processing`.
var errIdemUnavailable = errors.New("idempotency store unavailable")

// idempotency guards against duplicate sends across Kafka redelivery using a
// per-(schedule, attempt) Redis key set with NX + TTL.
type idempotency struct {
	rc  rueidis.Client
	ttl time.Duration
}

func NewIdempotency(rc rueidis.Client, ttl time.Duration) *idempotency {
	return &idempotency{rc: rc, ttl: ttl}
}

// Acquire attempts to claim the send for (scheduleID, retryCount).
//   - (true, nil)  → this worker owns the send; proceed.
//   - (false, nil) → another worker already owns/owned it; skip the provider call.
//   - (false, err) → Redis unreachable after retries; caller should leave the row.
func (s *idempotency) Acquire(ctx context.Context, scheduleID string, retryCount int) (bool, error) {
	key := fmt.Sprintf("idempotency:%s:%d", scheduleID, retryCount)
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 && !sleep(ctx, redisBackoffs[attempt-1]) {
			return false, ctx.Err()
		}
		cmd := s.rc.B().Set().Key(key).Value("1").Nx().
			ExSeconds(int64(s.ttl.Seconds())).Build()
		err := s.rc.Do(ctx, cmd).Error()
		if err == nil {
			return true, nil
		}
		if rueidis.IsRedisNil(err) {
			// SET NX returned nil → the key already existed.
			return false, nil
		}
		lastErr = err
	}
	return false, fmt.Errorf("%w: %v", errIdemUnavailable, lastErr)
}
