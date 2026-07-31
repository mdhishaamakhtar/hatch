package kafka

import (
	"testing"

	"github.com/google/uuid"
)

func TestDuePayloadRoundTrip(t *testing.T) {
	const scheduleID = "0195e2c0-0000-7000-8000-000000000001"

	raw := MarshalDuePayload(scheduleID)
	if want := `{"schedule_id":"` + scheduleID + `"}`; string(raw) != want {
		t.Fatalf("MarshalDuePayload = %s, want %s", raw, want)
	}
	if got := ScheduleIDFromValue(raw); got != scheduleID {
		t.Errorf("ScheduleIDFromValue = %q, want %q", got, scheduleID)
	}
}

// A malformed payload yields "" rather than an error, so callers can skip the
// record without an extra error branch.
func TestScheduleIDFromValueOnMalformedPayload(t *testing.T) {
	for _, bad := range []string{"", "not json", "[]", `{"other":"field"}`} {
		if got := ScheduleIDFromValue([]byte(bad)); got != "" {
			t.Errorf("ScheduleIDFromValue(%q) = %q, want empty", bad, got)
		}
	}
}

func TestNewDueRecordKeysOnTheBinaryID(t *testing.T) {
	id := uuid.New()
	rec := NewDueRecord(TopicEmailsDue, id[:], id.String())

	if rec.Topic != TopicEmailsDue {
		t.Errorf("topic = %q, want %q", rec.Topic, TopicEmailsDue)
	}
	// The key is the binary id so every message for a schedule lands on the same
	// partition and stays ordered.
	if len(rec.Key) != 16 {
		t.Fatalf("key length = %d, want the 16-byte binary uuid", len(rec.Key))
	}
	if got := ScheduleIDFromValue(rec.Value); got != id.String() {
		t.Errorf("payload schedule_id = %q, want %q", got, id)
	}
}

// The record must own its key: callers reuse and mutate the source buffer.
func TestNewDueRecordCopiesTheKey(t *testing.T) {
	src := make([]byte, 16)
	rec := NewDueRecord(TopicEmailsDue, src, "id")
	src[0] = 0xff
	if rec.Key[0] == 0xff {
		t.Fatal("record key aliases the caller's slice; want a copy")
	}
}

func TestParseBrokersTrimsAndDropsBlanks(t *testing.T) {
	cases := map[string][]string{
		"a:9092":             {"a:9092"},
		" a:9092 , b:9092 ,": {"a:9092", "b:9092"},
		",,a:1,,":            {"a:1"},
		"":                   {},
		"   ":                {},
	}
	for in, want := range cases {
		got := ParseBrokers(in)
		if len(got) != len(want) {
			t.Errorf("ParseBrokers(%q) = %v, want %v", in, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("ParseBrokers(%q) = %v, want %v", in, got, want)
				break
			}
		}
	}
}
