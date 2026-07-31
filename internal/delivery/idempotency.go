package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// errIdemUnavailable means the idempotency lock could not be reached because
// Redis was unreachable. The processor leaves the row `processing`.
var errIdemUnavailable = errors.New("idempotency store unavailable")

// claimState is what the Redis key for a (schedule, attempt) says about the send.
type claimState int

const (
	// claimFree — no one holds the key; this worker now owns the send.
	claimFree claimState = iota
	// claimInFlight — another worker claimed the send but has not confirmed it
	// completed. It may still be running, or it may have died mid-send. Either
	// way this worker must not touch the row: doing anything else would either
	// double-send or (worse) record a send that never happened.
	claimInFlight
	// claimSent — another worker claimed the send AND confirmed it went out.
	// Finishing the bookkeeping is safe and correct.
	claimSent
)

// Redis values for the two occupied states. The claim is written before the
// send and upgraded after it, which is what makes claimInFlight distinguishable
// from claimSent — without that distinction a failed acquire is ambiguous and
// an unsent email can be recorded as delivered.
const (
	claimValueClaimed = "claimed"
	claimValueSent    = "sent"
)

// idempotency guards against duplicate sends across Kafka redelivery using a
// per-(schedule, attempt) Redis key set with NX + TTL.
type idempotency struct {
	rc  rueidis.Client
	ttl time.Duration
}

func NewIdempotency(rc rueidis.Client, ttl time.Duration) *idempotency {
	return &idempotency{rc: rc, ttl: ttl}
}

func idemKey(scheduleID string, retryCount int) string {
	return fmt.Sprintf("idempotency:%s:%d", scheduleID, retryCount)
}

// Acquire attempts to claim the send for (scheduleID, retryCount), reporting
// what the key already said when the claim fails. A non-nil error means Redis
// was unreachable after retries and the caller should leave the row untouched.
func (s *idempotency) Acquire(ctx context.Context, scheduleID string, retryCount int) (claimState, error) {
	key := idemKey(scheduleID, retryCount)
	var lastErr error
	for attempt := range redisAttempts {
		if attempt > 0 && !sleep(ctx, redisBackoffs[attempt-1]) {
			return claimInFlight, ctx.Err()
		}
		cmd := s.rc.B().Set().Key(key).Value(claimValueClaimed).Nx().
			ExSeconds(int64(s.ttl.Seconds())).Build()
		err := s.rc.Do(ctx, cmd).Error()
		if err == nil {
			return claimFree, nil
		}
		if rueidis.IsRedisNil(err) {
			// SET NX returned nil → the key already existed. Read who holds it.
			return s.readClaim(ctx, key)
		}
		lastErr = err
	}
	return claimInFlight, fmt.Errorf("%w: %v", errIdemUnavailable, lastErr)
}

// MarkSent upgrades this worker's claim to "sent" once the provider has
// accepted the email, keeping the original TTL. After this, a later duplicate
// can safely complete the row's bookkeeping.
//
// A failure here is not fatal: the key stays "claimed", so the eventual retry
// re-sends rather than recording a delivery that may not have happened. That is
// the correct trade for an at-least-once system.
func (s *idempotency) MarkSent(ctx context.Context, scheduleID string, retryCount int) error {
	cmd := s.rc.B().Set().Key(idemKey(scheduleID, retryCount)).Value(claimValueSent).
		Xx().Keepttl().Build()
	err := s.rc.Do(ctx, cmd).Error()
	if err != nil && !rueidis.IsRedisNil(err) {
		return err
	}
	return nil
}

// readClaim resolves an occupied key to its state. An unreadable or unexpected
// value is reported as claimInFlight — the conservative reading, since treating
// an unknown claim as "sent" is what silently loses emails.
func (s *idempotency) readClaim(ctx context.Context, key string) (claimState, error) {
	v, err := s.rc.Do(ctx, s.rc.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			// Expired between the SET NX and the GET. Nothing is in flight, but
			// this attempt no longer owns the send either; the next redelivery or
			// reconciliation sweep picks it up cleanly.
			return claimInFlight, nil
		}
		return claimInFlight, fmt.Errorf("%w: %v", errIdemUnavailable, err)
	}
	if v == claimValueSent {
		return claimSent, nil
	}
	return claimInFlight, nil
}
