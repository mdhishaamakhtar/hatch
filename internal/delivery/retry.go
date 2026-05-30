package delivery

import (
	"context"
	"encoding/json"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka topics the delivery worker reads and writes. emails.due is the input;
// the retry tiers are produced to on transient failure (drained by the Phase 4
// retry consumers).
const (
	TopicEmailsDue  = "emails.due"
	TopicRetry1Min  = "emails.retry.1min"
	TopicRetry5Min  = "emails.retry.5min"
	TopicRetry30Min = "emails.retry.30min"
)

// duePayload is the JSON shape on emails.due and the retry tiers.
type duePayload struct {
	ScheduleID string `json:"schedule_id"`
}

// topicForRetry maps the post-increment retry_count to its tier topic:
// 1 → 1min, 2 → 5min, 3 (and any higher) → 30min.
func topicForRetry(retryCount int) string {
	switch retryCount {
	case 1:
		return TopicRetry1Min
	case 2:
		return TopicRetry5Min
	default:
		return TopicRetry30Min
	}
}

// retryTierLabel is the metric label for a tier topic.
func retryTierLabel(retryCount int) string {
	switch retryCount {
	case 1:
		return "1min"
	case 2:
		return "5min"
	default:
		return "30min"
	}
}

// produceRetry re-enqueues a schedule id onto the tier topic for retryCount,
// carrying the OTel trace context forward in the message headers.
func produceRetry(ctx context.Context, producer Producer, id [16]byte, scheduleID string, retryCount int) error {
	payload, _ := json.Marshal(duePayload{ScheduleID: scheduleID})
	rec := &kgo.Record{
		Topic: topicForRetry(retryCount),
		Key:   append([]byte(nil), id[:]...),
		Value: payload,
	}
	kafka.InjectOtelHeaders(ctx, rec)
	return producer.Produce(ctx, rec)
}

// scheduleIDFromValue extracts the schedule_id string from an emails.due record.
func scheduleIDFromValue(value []byte) string {
	var p duePayload
	if err := json.Unmarshal(value, &p); err != nil {
		return ""
	}
	return p.ScheduleID
}
