package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	dueTopic              = "emails.due"
	expectedDuePartitions = 12
	scheduleBatch         = 20
)

// shardStats is the slice of /internal/wheel/stats the verifier inspects.
type shardStats struct {
	PodIndex    int `json:"pod_index"`
	TotalPods   int `json:"total_pods"`
	TotalLoaded int `json:"total_loaded"`
}

// checkScheduler verifies the topic, the per-shard identity, and the full
// API → Postgres → scheduler → Kafka chain. It reaches each scheduler pod by
// its per-pod headless DNS and drives the wheel via POST /internal/poll instead
// of restarting the StatefulSet.
func (v *Verifier) checkScheduler(ctx context.Context) {
	v.rep.Section("Scheduler — topic, shards, schedule → Kafka")
	v.checkTopic(ctx)
	v.checkShardIdentity(ctx)
	v.checkScheduleToKafka(ctx)
}

// checkTopic asserts emails.due exists with the expected partition count via a
// Kafka MetadataRequest (no kubectl exec).
func (v *Verifier) checkTopic(ctx context.Context) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(v.cfg.Brokers...))
	if err != nil {
		v.rep.Failf("kafka metadata client: %v", err)
		return
	}
	defer cl.Close()

	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	topic := dueTopic
	t.Topic = &topic
	req.Topics = append(req.Topics, t)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		v.rep.Failf("kafka metadata request: %v", err)
		return
	}
	for _, rt := range resp.Topics {
		if rt.Topic == nil || *rt.Topic != dueTopic {
			continue
		}
		if rt.ErrorCode != 0 {
			// Code 3 = UNKNOWN_TOPIC_OR_PARTITION — the topic was never created
			// (commonly because the kafka-topics-bootstrap post-install hook
			// didn't run, e.g. the helm release failed). Re-run `make up`.
			v.rep.Failf("emails.due unavailable (Kafka error code %d — topic likely not created; re-run `make up`)", rt.ErrorCode)
			return
		}
		n := len(rt.Partitions)
		v.rep.Check(n == expectedDuePartitions,
			fmt.Sprintf("emails.due exists with %d partitions", n),
			fmt.Sprintf("emails.due partition count = %d, want %d", n, expectedDuePartitions))
		return
	}
	v.rep.Fail("emails.due topic missing")
}

// checkShardIdentity confirms each scheduler pod reports its own ordinal and
// the full replica count.
func (v *Verifier) checkShardIdentity(ctx context.Context) {
	for i := 0; i < v.cfg.SchedReplicas; i++ {
		s, err := v.fetchStats(ctx, i)
		if err != nil {
			v.rep.Failf("scheduler-%d stats: %v", i, err)
			continue
		}
		v.rep.Check(s.PodIndex == i && s.TotalPods == v.cfg.SchedReplicas,
			fmt.Sprintf("scheduler-%d stats reports pod_index=%d total_pods=%d", i, i, v.cfg.SchedReplicas),
			fmt.Sprintf("scheduler-%d stats wrong: pod_index=%d total_pods=%d", i, s.PodIndex, s.TotalPods))
	}
}

// checkScheduleToKafka posts a batch via the API, triggers an out-of-band poll
// on every shard, waits for the wheel to load them, then drains emails.due and
// asserts every posted schedule_id appears.
func (v *Verifier) checkScheduleToKafka(ctx context.Context) {
	if v.clientKey == "" {
		v.rep.Fail("no verify client available; skipping schedule → Kafka chain")
		return
	}

	lead := v.cfg.ScheduleLeadSeconds
	postTime := time.Now()
	deliverMs := postTime.Add(time.Duration(lead) * time.Second).UnixMilli()

	expected := make(map[string]bool, scheduleBatch)
	posted := 0
	for i := 1; i <= scheduleBatch; i++ {
		key := fmt.Sprintf("%s-batch-%d", v.runID, i)
		resp, err := v.do(ctx, http.MethodPost, v.cfg.APIBase+"/v1/schedules", v.clientKey, v.schedulePayload(deliverMs, key))
		if err != nil || resp.code != http.StatusCreated {
			v.rep.Failf("batch schedule %d → %v (code %d)", i, err, codeOf(resp, err))
			continue
		}
		if sid := jsonField(resp.body, "schedule_id"); sid != "" {
			expected[sid] = true
			v.postedIDs = append(v.postedIDs, sid)
			posted++
		}
	}
	v.rep.Check(posted == scheduleBatch,
		fmt.Sprintf("posted %d schedules (deliver_at = now+%ds)", posted, lead),
		fmt.Sprintf("posted %d of %d schedules", posted, scheduleBatch))

	// Trigger an immediate poll on every shard so the wheel picks up the rows
	// we just created (the hourly poller would otherwise not see them for ~1h).
	for i := 0; i < v.cfg.SchedReplicas; i++ {
		resp, err := v.do(ctx, http.MethodPost, v.cfg.SchedulerURL(i)+"/internal/poll", v.cfg.AdminKey, nil)
		v.rep.Check(err == nil && resp.code == http.StatusAccepted,
			fmt.Sprintf("POST scheduler-%d /internal/poll → 202", i),
			fmt.Sprintf("POST scheduler-%d /internal/poll → %d", i, codeOf(resp, err)))
	}

	// Wait until every shard has loaded at least one schedule.
	loadedAll := retry(ctx, 30, 2*time.Second, func() bool {
		hits := 0
		for i := 0; i < v.cfg.SchedReplicas; i++ {
			if s, err := v.fetchStats(ctx, i); err == nil && s.TotalLoaded > 0 {
				hits++
			}
		}
		return hits == v.cfg.SchedReplicas
	})
	v.rep.Check(loadedAll,
		fmt.Sprintf("all %d scheduler shards loaded ≥1 schedule", v.cfg.SchedReplicas),
		"not all scheduler shards loaded a schedule after poll")

	// Wait for the schedules to fire, then drain the topic.
	fireAt := postTime.Add(time.Duration(lead+10) * time.Second)
	if d := time.Until(fireAt); d > 0 {
		fmt.Printf("  waiting %s for schedules to mature on the wheel…\n", d.Round(time.Second))
		select {
		case <-ctx.Done():
			v.rep.Fail("context cancelled while waiting for deliver_at")
			return
		case <-time.After(d):
		}
	}
	v.consume(ctx, expected)
}

// consume drains emails.due with a throwaway consumer group (unique per run,
// earliest offset, no commit) and asserts every expected schedule_id is seen.
func (v *Verifier) consume(ctx context.Context, expected map[string]bool) {
	if len(expected) == 0 {
		v.rep.Fail("no schedules posted; nothing to consume")
		return
	}
	cl, err := hkafka.NewConsumer(v.cfg.Brokers, v.runID, []string{dueTopic}, v.lg)
	if err != nil {
		v.rep.Failf("kafka consumer: %v", err)
		return
	}
	defer cl.Close()

	seen := make(map[string]bool, len(expected))
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for len(seen) < len(expected) {
		fetches := cl.PollFetches(cctx)
		if len(fetches.Errors()) > 0 {
			break // deadline/cancel — stop draining
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if sid := jsonField(r.Value, "schedule_id"); expected[sid] {
				seen[sid] = true
			}
		})
	}
	v.rep.Check(len(seen) == len(expected),
		fmt.Sprintf("consumed all %d expected schedule_ids on %s", len(expected), dueTopic),
		fmt.Sprintf("consumed %d of %d expected schedule_ids on %s", len(seen), len(expected), dueTopic))
}

// fetchStats GETs the per-pod wheel stats over headless DNS.
func (v *Verifier) fetchStats(ctx context.Context, ordinal int) (shardStats, error) {
	resp, err := v.do(ctx, http.MethodGet, v.cfg.SchedulerURL(ordinal)+"/internal/wheel/stats", v.cfg.AdminKey, nil)
	if err != nil {
		return shardStats{}, err
	}
	if resp.code != http.StatusOK {
		return shardStats{}, fmt.Errorf("code %d", resp.code)
	}
	var s shardStats
	if err := json.Unmarshal(resp.body, &s); err != nil {
		return shardStats{}, err
	}
	return s, nil
}
