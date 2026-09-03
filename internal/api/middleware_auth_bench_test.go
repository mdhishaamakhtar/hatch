package api

import (
	"testing"
)

// sinkBytes stops the compiler from eliminating the work under test.
var sinkBytes []byte

const benchAPIKey = "hatch_sk_0123456789abcdef0123456789abcdef"

// BenchmarkClientAuthDigest measures the whole in-process cost of authenticating
// a request: the sha256 of the bearer token, which is both the value the client
// row is looked up by and the credential check itself.
//
// It runs on every authenticated /v1 request and nothing caches it, so this is
// the per-request price of auth. At tens of nanoseconds it is far below the
// indexed Postgres lookup that follows, which means the ingest path is bounded
// by the database rather than by the credential check.
func BenchmarkClientAuthDigest(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sinkBytes = sha256Bytes(benchAPIKey)
	}
}
