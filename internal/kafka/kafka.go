// Package kafka defines the topics Hatch services hand schedules to each other
// on, the records they exchange, and how every service builds its clients.
//
// A record names one schedule and nothing else: its value is the schedule id,
// and all delivery state lives in Postgres. The binary id is the record key, so
// every record for a schedule lands on the same partition.
package kafka

import (
	"context"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// TopicDue carries schedules that are due to be sent. The scheduler and the
// reconciliation cron produce to it, and the delivery workers consume it.
const TopicDue = "emails.due"

// A RetryTier is one rung of the retry ladder: a topic the delivery worker parks
// a failed send on, which the retry consumer drains back onto TopicDue.
type RetryTier struct {
	Name  string // the tier's label in metrics and consumer group names
	Topic string
}

// RetryTiers are the tiers a failing send climbs, in order.
var RetryTiers = []RetryTier{
	{Name: "1min", Topic: "emails.retry.1min"},
	{Name: "5min", Topic: "emails.retry.5min"},
	{Name: "30min", Topic: "emails.retry.30min"},
}

// DueRecord builds a record asking for schedule id to be delivered, carrying the
// trace in ctx so the consumer can continue it.
func DueRecord(ctx context.Context, topic string, id uuid.UUID) *kgo.Record {
	r := &kgo.Record{Topic: topic, Key: id[:], Value: []byte(id.String())}
	otel.GetTextMapPropagator().Inject(ctx, kotel.NewRecordCarrier(r))
	return r
}

// ScheduleID returns the schedule a record built by DueRecord refers to.
func ScheduleID(r *kgo.Record) (uuid.UUID, error) {
	return uuid.ParseBytes(r.Value)
}

// ExtractTrace returns ctx carrying the trace that produced r.
func ExtractTrace(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kotel.NewRecordCarrier(r))
}

// NewProducer returns a client for producing records.
func NewProducer(brokers []string, lg *zap.Logger) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.WithLogger(kzap.New(lg)))
}

// NewConsumer returns a client that consumes topic as a member of group. Offsets
// are never committed automatically: the caller commits records once it has
// handled them, which is what makes consumption at-least-once.
func NewConsumer(brokers []string, group, topic string, lg *zap.Logger) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kzap.New(lg)),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
}
