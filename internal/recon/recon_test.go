package recon

import (
	"context"
	"errors"
	"testing"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type fakeStore struct {
	unattempted, orphaned [][]byte
	err                   error
}

func (f fakeStore) RecoverUnattempted(context.Context) ([][]byte, error)     { return f.unattempted, f.err }
func (f fakeStore) RecoverOrphanedRetries(context.Context) ([][]byte, error) { return f.orphaned, nil }

type fakeProducer struct{ produced []*kgo.Record }

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.produced = append(f.produced, rs...)
	var out kgo.ProduceResults
	for _, r := range rs {
		out = append(out, kgo.ProduceResult{Record: r})
	}
	return out
}

func ids(n int) ([][]byte, map[uuid.UUID]bool) {
	raw := make([][]byte, n)
	set := make(map[uuid.UUID]bool, n)
	for i := range raw {
		id := uuid.New()
		raw[i], set[id] = id[:], true
	}
	return raw, set
}

func TestSweepRequeuesWhatBothPassesRecover(t *testing.T) {
	unattempted, want := ids(2)
	orphaned, more := ids(1)
	for id := range more {
		want[id] = true
	}
	prod := &fakeProducer{}

	u, o, err := Sweep(context.Background(), zap.NewNop(), fakeStore{unattempted: unattempted, orphaned: orphaned}, prod)
	if err != nil || u != 2 || o != 1 {
		t.Fatalf("Sweep = (%d, %d, %v), want (2, 1, nil)", u, o, err)
	}
	if len(prod.produced) != 3 {
		t.Fatalf("produced %d records, want 3", len(prod.produced))
	}
	for _, r := range prod.produced {
		id, err := kafka.ScheduleID(r)
		if err != nil || !want[id] || r.Topic != kafka.TopicDue {
			t.Errorf("unexpected record %s on %s", r.Value, r.Topic)
		}
	}
}

func TestSweepStopsWhenAQueryFails(t *testing.T) {
	prod := &fakeProducer{}
	if _, _, err := Sweep(context.Background(), zap.NewNop(), fakeStore{err: errors.New("db down")}, prod); err == nil {
		t.Fatal("Sweep succeeded despite the query failing")
	}
	if len(prod.produced) != 0 {
		t.Errorf("produced %d records after a failed query", len(prod.produced))
	}
}
