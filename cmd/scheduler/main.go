// scheduler-service — Hatch Phase 2 service. Polls Postgres for this pod's
// hash slice of pending schedules, incubates them in an in-memory wheel
// persisted to bbolt, and produces `emails.due` to Kafka at the exact second
// each schedule matures. See internal/scheduler for the goroutine pipeline.
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/internal/scheduler"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() { service.Main("scheduler-service", run) }

// seedPodIndex populates POD_INDEX from the pod hostname when running inside a
// StatefulSet with a distroless image (no shell available for the wrapper trick).
// The hostname of a StatefulSet pod is always "<name>-<ordinal>".
func seedPodIndex() {
	if os.Getenv("POD_INDEX") != "" {
		return
	}
	host, err := os.Hostname()
	if err != nil {
		return
	}
	_ = os.Setenv("POD_INDEX", host[strings.LastIndex(host, "-")+1:])
}

func run(lg *zap.Logger) error {
	seedPodIndex()
	cfg, err := config.Load[scheduler.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if cfg.TotalPods <= 0 {
		return fmt.Errorf("TOTAL_PODS must be > 0 (got %d)", cfg.TotalPods)
	}
	if cfg.PodIndex < 0 || cfg.PodIndex >= cfg.TotalPods {
		return fmt.Errorf("POD_INDEX %d out of range for TOTAL_PODS=%d", cfg.PodIndex, cfg.TotalPods)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "scheduler-service", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()
	tr := otel.Tracer("scheduler-service")

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	prodCl, err := hkafka.NewProducer(cfg.Brokers(), lg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prodCl.Close()

	store, err := wheelstore.Open(cfg.WheelDBPath)
	if err != nil {
		return fmt.Errorf("wheelstore open: %w", err)
	}
	defer func() { _ = store.Close() }()

	wheel := scheduler.NewWheel()
	if err := scheduler.Recover(lg, wheel, store, time.Now()); err != nil {
		return fmt.Errorf("wheel recovery: %w", err)
	}

	pipeline := scheduler.NewPipeline(cfg, lg, wheel, store, gen.New(pool),
		hkafka.NewRecordProducer(prodCl), tr)

	scheduler.RecordPodIdentity(cfg.PodIndex, cfg.TotalPods)

	go pipeline.RunPoller(ctx, nil)
	go pipeline.RunBuilder(ctx)
	go pipeline.RunTicker(ctx, nil)

	srv := scheduler.NewServer(cfg, lg, pool, pipeline, func() bool { return true })
	return service.Serve(ctx, lg, "scheduler-service", cfg.AdminPort, srv.Handler(), cfg.ShutdownTimeout,
		zap.Int("pod_index", cfg.PodIndex),
		zap.Int("total_pods", cfg.TotalPods),
	)
}
