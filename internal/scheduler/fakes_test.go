package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/wheelstore"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// id builds a recognisable 16-byte schedule id filled with b.
func id(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// fakeStore is an in-memory WheelStore. Thread-safe: the builder goroutine
// writes to it while the test goroutine reads.
type fakeStore struct {
	mu      sync.Mutex
	data    map[string][]wheelstore.Entry
	deletes []string
}

func newFakeStore() *fakeStore { return &fakeStore{data: map[string][]wheelstore.Entry{}} }

func (f *fakeStore) Append(slot string, entryID [16]byte, deliverAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[slot] = append(f.data[slot], wheelstore.Entry{ID: entryID, DeliverAt: deliverAt})
	return nil
}

func (f *fakeStore) Delete(slot string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, slot)
	f.deletes = append(f.deletes, slot)
	return nil
}

func (f *fakeStore) Range(fn func(string, []wheelstore.Entry) error) error {
	for slot, entries := range f.snapshot() {
		if err := fn(slot, entries); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) deleteLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletes...)
}

func (f *fakeStore) snapshot() map[string][]wheelstore.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]wheelstore.Entry, len(f.data))
	for slot, entries := range f.data {
		out[slot] = append([]wheelstore.Entry(nil), entries...)
	}
	return out
}

// fakePoller returns canned rows and counts how many poll cycles ran.
type fakePoller struct {
	mu   sync.Mutex
	rows []gen.PollHourWindowRow
	hits int
}

func (f *fakePoller) PollHourWindow(context.Context, gen.PollHourWindowParams) ([]gen.PollHourWindowRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	return f.rows, nil
}

func (f *fakePoller) hitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

func pollRow(b byte, at time.Time) gen.PollHourWindowRow {
	rowID := id(b)
	return gen.PollHourWindowRow{
		ID:        rowID[:],
		DeliverAt: pgtype.Timestamptz{Time: at, Valid: true},
	}
}

// fakeProducer collects every produced record. A non-nil err fails every
// Produce so the failure path can be exercised.
type fakeProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
	err     error
}

func (p *fakeProducer) Produce(_ context.Context, r *kgo.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, r)
	return p.err
}

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

func (p *fakeProducer) snapshot() []*kgo.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kgo.Record(nil), p.records...)
}

// testPipeline wires a Pipeline over fakes with small channel buffers.
func testPipeline(cfg Config, store WheelStore, poller SchedulePoller, producer MessageProducer) *Pipeline {
	if cfg.ScheduleChannelBuffer == 0 {
		cfg.ScheduleChannelBuffer = 8
	}
	if cfg.ClearChannelBuffer == 0 {
		cfg.ClearChannelBuffer = 4
	}
	if cfg.TotalPods == 0 {
		cfg.TotalPods = 1
	}
	return NewPipeline(cfg, zap.NewNop(), NewWheel(), store, poller, producer,
		noop.NewTracerProvider().Tracer("test"))
}

// eventually spins until cond holds or the budget expires. Used instead of a
// fixed sleep so the tests stay fast and don't flake under load.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
