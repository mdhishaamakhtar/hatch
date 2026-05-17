package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestWriteError(t *testing.T) {
	rr := httptest.NewRecorder()
	writeError(rr, 418, "teapot", "short_and_stout")
	if rr.Code != 418 {
		t.Fatalf("status: want 418 got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: %q", ct)
	}
	var body apiError
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Error != "teapot" || body.Reason != "short_and_stout" {
		t.Fatalf("body: %+v", body)
	}
}

func TestWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, 201, map[string]string{"hello": "world"})
	if rr.Code != 201 {
		t.Fatalf("status: %d", rr.Code)
	}
	var m map[string]string
	_ = json.NewDecoder(rr.Body).Decode(&m)
	if m["hello"] != "world" {
		t.Fatalf("body: %+v", m)
	}
}
