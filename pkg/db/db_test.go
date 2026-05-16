package db

import (
	"testing"

	"github.com/google/uuid"
)

func TestUUIDRoundTrip(t *testing.T) {
	in := uuid.New()
	b := UUIDToBytes(in)
	if len(b) != 16 {
		t.Fatalf("len = %d, want 16", len(b))
	}
	out, err := BytesToUUID(b)
	if err != nil {
		t.Fatalf("BytesToUUID: %v", err)
	}
	if in != out {
		t.Errorf("round trip mismatch: in=%s out=%s", in, out)
	}
}

func TestBytesToUUID_wrongLen(t *testing.T) {
	if _, err := BytesToUUID(make([]byte, 15)); err == nil {
		t.Error("expected error for short buffer")
	}
	if _, err := BytesToUUID(make([]byte, 17)); err == nil {
		t.Error("expected error for long buffer")
	}
}
