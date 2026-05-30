package delivery

import "testing"

func TestTopicForRetry(t *testing.T) {
	cases := map[int]string{
		1: TopicRetry1Min,
		2: TopicRetry5Min,
		3: TopicRetry30Min,
		4: TopicRetry30Min, // anything beyond tier 3 stays on 30min
	}
	for n, want := range cases {
		if got := topicForRetry(n); got != want {
			t.Errorf("topicForRetry(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestRetryTierLabel(t *testing.T) {
	cases := map[int]string{1: "1min", 2: "5min", 3: "30min", 9: "30min"}
	for n, want := range cases {
		if got := retryTierLabel(n); got != want {
			t.Errorf("retryTierLabel(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestScheduleIDFromValue(t *testing.T) {
	if got := scheduleIDFromValue([]byte(`{"schedule_id":"abc-123"}`)); got != "abc-123" {
		t.Errorf("got %q, want abc-123", got)
	}
	if got := scheduleIDFromValue([]byte(`not json`)); got != "" {
		t.Errorf("malformed payload should yield empty, got %q", got)
	}
}
