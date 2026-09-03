package api

import (
	"fmt"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Sinks stop the compiler from eliminating the work under test.
var (
	sinkErr   error
	sinkBytes []byte
)

const benchAPIKey = "hatch_sk_0123456789abcdef0123456789abcdef"

// BenchmarkClientAuthBcryptCompare measures the bcrypt verification ClientAuth
// performs on every authenticated /v1 request. Nothing caches it: the middleware
// hits Postgres for the row and then compares the bcrypt hash on each request,
// so this cost is paid per request for the life of the pod.
//
// That makes this single operation the API's ingest ceiling — a pod can serve at
// most cores/duration requests per second no matter how fast Postgres is. The
// cost sweep is the actionable part: BCRYPT_COST defaults to 12, and each step
// down halves the work.
func BenchmarkClientAuthBcryptCompare(b *testing.B) {
	for _, cost := range []int{8, 10, 12} {
		hash, err := bcrypt.GenerateFromPassword([]byte(benchAPIKey), cost)
		if err != nil {
			b.Fatalf("GenerateFromPassword(cost=%d): %v", cost, err)
		}
		b.Run(fmt.Sprintf("cost=%d", cost), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkErr = bcrypt.CompareHashAndPassword(hash, []byte(benchAPIKey))
			}
		})
	}
}

// BenchmarkClientAuthLookupHash measures the other half of the auth path: the
// sha256 of the bearer token used to hit the unique index on
// clients.api_key_lookup.
//
// Reported next to the bcrypt numbers on purpose. The two run on the same
// request, and the ratio between them is the whole story — the indexable hash is
// free, and every millisecond of auth is the deliberately-slow comparison.
func BenchmarkClientAuthLookupHash(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sinkBytes = sha256Bytes(benchAPIKey)
	}
}
