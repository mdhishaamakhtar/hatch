package kafka

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The Hatch topic names. These are a cross-service contract (HLD §Topics): the
// scheduler and the reconciliation cron produce to TopicEmailsDue, the delivery
// worker consumes it and produces to the retry tiers, and the retry consumers
// drain the tiers back onto TopicEmailsDue.
const (
	TopicEmailsDue  = "emails.due"
	TopicRetry1Min  = "emails.retry.1min"
	TopicRetry5Min  = "emails.retry.5min"
	TopicRetry30Min = "emails.retry.30min"
)

// DuePayload is the JSON body carried on emails.due and every retry tier — a
// thin envelope of just the schedule id. All delivery state lives in Postgres,
// so the message never has to be kept in sync with the row.
type DuePayload struct {
	ScheduleID string `json:"schedule_id"`
}

// MarshalDuePayload encodes the envelope for a schedule id. The struct has only
// a string field, so marshalling cannot fail.
func MarshalDuePayload(scheduleID string) []byte {
	b, _ := json.Marshal(DuePayload{ScheduleID: scheduleID})
	return b
}

// ScheduleIDFromValue extracts the schedule id from a record value. Returns ""
// on a malformed payload so callers can skip (or log) without an error branch.
func ScheduleIDFromValue(value []byte) string {
	var p DuePayload
	if err := json.Unmarshal(value, &p); err != nil {
		return ""
	}
	return p.ScheduleID
}

// ParseBrokers splits a KAFKA_BROKERS csv into addresses, trimming whitespace
// and dropping empty entries. Every service's Config.Brokers() calls this.
func ParseBrokers(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Producer is the narrow produce surface every Hatch service needs. The
// synchronous signature lets callers handle produce errors inline (and decide
// whether to commit). Tests substitute a fake.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record) error
}

// clientProducer adapts *kgo.Client to Producer so service code stays decoupled
// from franz-go specifics.
type clientProducer struct{ cl *kgo.Client }

// NewRecordProducer wraps a franz-go client so it satisfies Producer.
func NewRecordProducer(cl *kgo.Client) Producer { return clientProducer{cl: cl} }

func (p clientProducer) Produce(ctx context.Context, r *kgo.Record) error {
	return p.cl.ProduceSync(ctx, r).FirstErr()
}

// NewDueRecord builds an emails.due (or retry-tier) record for a schedule. The
// 16-byte binary id is the partition key so every message for a schedule lands
// on the same partition and is processed in order.
func NewDueRecord(topic string, id []byte, scheduleID string) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   append([]byte(nil), id...),
		Value: MarshalDuePayload(scheduleID),
	}
}
