package delivery

import "testing"

func TestIdemKeyIncludesTheAttempt(t *testing.T) {
	// The key is per-attempt: a retry must be able to claim its own send even
	// though the previous attempt's key still exists.
	first := idemKey("abc", 0)
	second := idemKey("abc", 1)
	if first == second {
		t.Fatalf("keys for different attempts collided: %q", first)
	}
	if want := "idempotency:abc:0"; first != want {
		t.Errorf("idemKey = %q, want %q", first, want)
	}
}
