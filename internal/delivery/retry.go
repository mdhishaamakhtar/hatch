package delivery

import (
	"context"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
)

// retryTiers maps a post-increment retry_count to the tier it lands in: 1 →
// 1min, 2 → 5min, 3 and beyond → 30min. One table so the topic and its metric
// label can never disagree.
var retryTiers = []struct {
	label string
	topic string
}{
	{"1min", kafka.TopicRetry1Min},
	{"5min", kafka.TopicRetry5Min},
	{"30min", kafka.TopicRetry30Min},
}

// tierFor returns the tier for a retry count, clamping anything past the last
// tier onto it.
func tierFor(retryCount int) (label, topic string) {
	i := min(max(retryCount, 1), len(retryTiers)) - 1
	return retryTiers[i].label, retryTiers[i].topic
}

// produceRetry re-enqueues a schedule id onto the tier topic for retryCount,
// carrying the OTel trace context forward in the message headers.
func produceRetry(ctx context.Context, producer kafka.Producer, id []byte, scheduleID string, retryCount int) error {
	_, topic := tierFor(retryCount)
	rec := kafka.NewDueRecord(topic, id, scheduleID)
	kafka.InjectOtelHeaders(ctx, rec)
	return producer.Produce(ctx, rec)
}
