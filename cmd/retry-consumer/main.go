// retry-consumer — Hatch Phase 4 service. Runs one drain goroutine per retry
// tier (emails.retry.1min / 5min / 30min): each drains its topic on a schedule
// and re-enqueues every schedule_id to emails.due with a fresh OTel context.
// Stateless — no Postgres or Redis. See internal/retry for the design.
package main

import (
	"fmt"

	"github.com/mdhishaamakhtar/hatch/internal/retry"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() { service.Main("retry-consumer", run) }

func run(lg *zap.Logger) error {
	cfg, err := config.Load[retry.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "retry-consumer", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()
	tr := otel.Tracer("retry-consumer")

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()
	producer := hkafka.NewRecordProducer(prodCl)

	// One durable-group consumer + drain goroutine per tier.
	var consumers []*kgo.Client
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()
	for _, tier := range cfg.Tiers() {
		consumer, err := hkafka.NewConsumer(cfg.Brokers(), tier.Group, []string{tier.Topic}, lg)
		if err != nil {
			return fmt.Errorf("kafka consumer (%s): %w", tier.Name, err)
		}
		consumers = append(consumers, consumer)
		go retry.NewDrainer(tier, consumer, producer, tr, lg, cfg).Run(ctx)
	}

	return service.Serve(ctx, lg, "retry-consumer", cfg.AdminPort,
		retry.AdminHandler(prodCl), cfg.ShutdownTimeout)
}
