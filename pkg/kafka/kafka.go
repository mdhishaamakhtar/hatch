// Package kafka centralises every Hatch service's Kafka producer/consumer
// construction and the OTel propagation helpers used to thread trace context
// through `emails.due` and the retry tiers.
//
// Producer config: acks=all (required by idempotent mode), linger 5ms, max
// batch 1MiB. The same NewProducer is reused by the Phase 5 reconciliation
// cron and the retry-consumer re-enqueue path.
package kafka

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducer dials the broker list and returns a configured kgo.Client.
// Caller owns the returned client and must Close() it on shutdown.
func NewProducer(brokers []string, lg *zap.Logger) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1_048_576),
		kgo.WithLogger(kzap{lg: lg}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return cl, nil
}

// NewConsumer dials the broker list and returns a kgo.Client bound to a
// consumer group reading the given topics from the earliest offset with
// auto-commit disabled. This is the throwaway-group shape the verifier uses to
// drain a topic without persisting offsets or depending on prior group state;
// callers pass a unique, disposable group id. Caller owns the returned client
// and must Close() it.
func NewConsumer(brokers []string, group string, topics []string, lg *zap.Logger) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(kzap{lg: lg}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return cl, nil
}

// kzap adapts *zap.Logger to franz-go's kgo.Logger interface so franz-go's
// internal messages land in Loki with the same JSON shape as the rest of the
// service's logs.
type kzap struct{ lg *zap.Logger }

func (k kzap) Level() kgo.LogLevel { return kgo.LogLevelInfo }
func (k kzap) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i+1 < len(keyvals); i += 2 {
		key, _ := keyvals[i].(string)
		fields = append(fields, zap.Any(key, keyvals[i+1]))
	}
	switch level {
	case kgo.LogLevelError:
		k.lg.Error(msg, fields...)
	case kgo.LogLevelWarn:
		k.lg.Warn(msg, fields...)
	case kgo.LogLevelDebug:
		k.lg.Debug(msg, fields...)
	default:
		k.lg.Info(msg, fields...)
	}
}

// headerCarrier adapts kgo.Record.Headers to the OTel TextMapCarrier interface
// so otel.GetTextMapPropagator() can read/write trace context onto Kafka
// messages without the caller building a map.
type headerCarrier struct{ r *kgo.Record }

func (h headerCarrier) Get(key string) string {
	for _, kv := range h.r.Headers {
		if kv.Key == key {
			return string(kv.Value)
		}
	}
	return ""
}

func (h headerCarrier) Set(key, value string) {
	for i, kv := range h.r.Headers {
		if kv.Key == key {
			h.r.Headers[i].Value = []byte(value)
			return
		}
	}
	h.r.Headers = append(h.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (h headerCarrier) Keys() []string {
	out := make([]string, 0, len(h.r.Headers))
	for _, kv := range h.r.Headers {
		out = append(out, kv.Key)
	}
	return out
}

// InjectOtelHeaders writes the current OTel span context onto the record's
// headers using the globally registered propagator (traceparent + tracestate).
// Call this once per produce so consumers can resume the trace.
func InjectOtelHeaders(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{r: r})
}

// ExtractOtelHeaders returns a context with the span context decoded from the
// record's headers. Used by Phase 3+ consumers.
func ExtractOtelHeaders(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{r: r})
}

// Static interface check — keeps the compiler honest if propagation drops the
// TextMapCarrier shape on us.
var _ propagation.TextMapCarrier = headerCarrier{}
