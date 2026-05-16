package redis

import "testing"

func TestNewClient_badAddr(t *testing.T) {
	if _, err := NewClient(""); err == nil {
		t.Error("expected error for empty addr")
	}
}
