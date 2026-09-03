package delivery

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Sinks stop the compiler from eliminating the work under test.
var (
	sinkSelection Selection
	sinkIDs       [][]byte
)

// BenchmarkRouterSelect measures the per-send routing decision: scan the
// client's providers, skip open breakers, pick the highest-token vendor, take a
// token. It runs under the Router's write lock, so it is the one part of the
// send path that every delivery goroutine on a pod would contend on.
//
// The vendor count is swept because Select is linear in the client's provider
// list. Buckets are given a large capacity so the benchmark measures selection
// rather than repeatedly bottoming out at zero tokens.
func BenchmarkRouterSelect(b *testing.B) {
	for _, n := range []int{1, 3, 5} {
		vendors := make([]string, n)
		for i := range vendors {
			vendors[i] = fmt.Sprintf("vendor%d", i)
		}
		r := testRouter(1<<30, 1<<30, factories(vendors...))
		ps := provs(vendors...)
		const clientID = "bench-client"
		r.Select(clientID, ps, "") // warm the per-vendor state map

		b.Run(fmt.Sprintf("vendors=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkSelection = r.Select(clientID, ps, "")
			}
		})
	}
}

// BenchmarkRouterSelectExcludingLastProvider covers the retry path, where the
// vendor that just failed is excluded. A single-provider client makes Select
// run its scan twice — once excluding, then again without the exclusion once it
// finds no alternative — so this is the worst case of the two-pass logic.
func BenchmarkRouterSelectExcludingLastProvider(b *testing.B) {
	r := testRouter(1<<30, 1<<30, factories("resend"))
	ps := provs("resend")
	const clientID = "bench-client"
	r.Select(clientID, ps, "")

	b.ReportAllocs()
	for b.Loop() {
		sinkSelection = r.Select(clientID, ps, "resend")
	}
}

// BenchmarkParseBatch measures the per-poll decode the processor runs before it
// can fetch anything: unmarshal every record's JSON envelope, parse the uuid,
// and build the id list plus the record lookup.
//
// Swept over the batch sizes the worker actually sees — DELIVERY_BATCH_SIZE
// defaults to 1000. ns/op is per batch, so divide by the record count for the
// per-message cost.
func BenchmarkParseBatch(b *testing.B) {
	for _, n := range []int{100, 1_000} {
		recs := make([]*kgo.Record, n)
		for i := range recs {
			var raw [16]byte
			binary.BigEndian.PutUint64(raw[:8], uint64(i))
			id := uuid.UUID(raw)
			recs[i] = kafka.NewDueRecord(kafka.TopicEmailsDue, raw[:], id.String())
		}
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ids, _ := parseBatch(recs)
				sinkIDs = ids
			}
		})
	}
}
