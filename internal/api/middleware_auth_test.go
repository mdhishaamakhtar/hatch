package api

import "testing"

// sha256Bytes backs the indexed api_key_lookup column, so it must be stable
// across processes and collision-free for distinct keys.
func TestSha256BytesIsDeterministic(t *testing.T) {
	a := sha256Bytes("hello")
	if string(a) != string(sha256Bytes("hello")) {
		t.Fatal("the same key must always hash to the same lookup value")
	}
	if string(a) == string(sha256Bytes("hello!")) {
		t.Fatal("different keys must hash differently")
	}
	if len(a) != 32 {
		t.Errorf("lookup value is %d bytes, want a full 32-byte sha256", len(a))
	}
}
