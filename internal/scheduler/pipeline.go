package scheduler

import (
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Pipeline is the scheduler's three-goroutine engine and everything they share.
//
//   - G1 RunPoller  — polls Postgres for this pod's slice, emits Entries.
//   - G2 RunBuilder — sole writer of the wheel and bbolt; drains entries and
//     cleared-slot notifications.
//   - G3 RunTicker  — fires the matching wheel slot each second to Kafka.
//
// Grouping them here rather than passing the same seven dependencies into three
// free functions keeps the wiring in one place and the run loops readable.
type Pipeline struct {
	cfg      Config
	lg       *zap.Logger
	wheel    *Wheel
	store    WheelStore
	queries  SchedulePoller
	producer MessageProducer
	tracer   trace.Tracer

	entries chan Entry  // G1 → G2: schedules to load into the wheel
	cleared chan string // G3 → G2: "MM:SS" slots that have fired and can be dropped from bbolt
	pollNow chan struct{}

	// now is the clock G3 reads. Tests substitute it; nil means time.Now.
	now func() time.Time
}

// NewPipeline wires the engine. Channel buffers come from cfg: entries is large
// (a poll can load an hour of work at once) and cleared is small (one slot per
// second). pollNow holds a single pending signal so the admin endpoint's
// non-blocking send always lands.
func NewPipeline(cfg Config, lg *zap.Logger, wheel *Wheel, store WheelStore, queries SchedulePoller, producer MessageProducer, tracer trace.Tracer) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		lg:       lg,
		wheel:    wheel,
		store:    store,
		queries:  queries,
		producer: producer,
		tracer:   tracer,
		entries:  make(chan Entry, cfg.ScheduleChannelBuffer),
		cleared:  make(chan string, cfg.ClearChannelBuffer),
		pollNow:  make(chan struct{}, 1),
	}
}

// PollTrigger is the channel POST /internal/poll sends on to force an
// out-of-band poll cycle.
func (p *Pipeline) PollTrigger() chan<- struct{} { return p.pollNow }

// PollPending reports whether an out-of-band poll is queued but not yet served.
func (p *Pipeline) PollPending() bool { return len(p.pollNow) > 0 }

// clock returns the pipeline's time source.
func (p *Pipeline) clock() time.Time {
	if p.now == nil {
		return time.Now()
	}
	return p.now()
}

// publishWheelGauges refreshes the occupancy gauges. Stats() is O(3600), which
// is cheap enough to run on every insert and every tick.
func (p *Pipeline) publishWheelGauges() {
	occupied, total := p.wheel.Stats()
	label := podLabel(p.cfg.PodIndex)
	mWheelOccupied.WithLabelValues(label).Set(float64(occupied))
	mWheelTotalLoaded.WithLabelValues(label).Set(float64(total))
}
