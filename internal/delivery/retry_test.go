package delivery

import (
	"testing"

	"github.com/mdhishaamakhtar/hatch/pkg/kafka"
)

func TestTierForClampsToTheLastTier(t *testing.T) {
	cases := []struct {
		retryCount int
		wantLabel  string
		wantTopic  string
	}{
		{1, "1min", kafka.TopicRetry1Min},
		{2, "5min", kafka.TopicRetry5Min},
		{3, "30min", kafka.TopicRetry30Min},
		{9, "30min", kafka.TopicRetry30Min}, // anything past the last tier stays there
		{0, "1min", kafka.TopicRetry1Min},   // defensive: never index below the first
	}
	for _, c := range cases {
		label, topic := tierFor(c.retryCount)
		if label != c.wantLabel || topic != c.wantTopic {
			t.Errorf("tierFor(%d) = (%q, %q), want (%q, %q)",
				c.retryCount, label, topic, c.wantLabel, c.wantTopic)
		}
	}
}
