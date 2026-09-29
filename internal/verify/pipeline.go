package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// batchSize is how many schedules the scheduler check sends through the wheel.
// With this many, every shard gets some of them.
const batchSize = 20

// checkScheduler takes a batch of schedules from the API through every shard's
// wheel and onto emails.due.
func (v *verifier) checkScheduler(ctx context.Context) {
	v.section("Scheduler")

	for i := range v.SchedulerReplicas {
		stats, err := v.wheelStats(ctx, i)
		if err != nil {
			v.fail("scheduler %d: %v", i, err)
			continue
		}
		v.check(stats.PodIndex == i && stats.TotalPods == v.SchedulerReplicas,
			"scheduler %d runs shard %d of %d", i, stats.PodIndex, stats.TotalPods)
	}
	if v.clientKey == "" {
		v.fail("no client to schedule as")
		return
	}

	since := time.Now()
	deliverAt := since.Add(v.cfg.ScheduleLead)
	for i := range batchSize {
		id, err := v.createSchedule(ctx, v.clientKey, v.email(deliverAt, fmt.Sprintf("recipient+%d@example.com", i)))
		if err != nil {
			v.fail("create schedule %d: %v", i, err)
			continue
		}
		v.fired = append(v.fired, id)
	}
	v.check(len(v.fired) == batchSize, "created %d of %d schedules, due in %s", len(v.fired), batchSize, v.cfg.ScheduleLead)
	if err := v.Poll(ctx); err != nil {
		v.fail("%v", err)
		return
	}
	loaded := eventually(ctx, time.Minute, func() bool {
		for i := range v.SchedulerReplicas {
			if stats, err := v.wheelStats(ctx, i); err != nil || stats.TotalLoaded == 0 {
				return false
			}
		}
		return true
	})
	v.check(loaded, "every shard loaded schedules onto its wheel")

	fmt.Printf("  waiting up to %s for them to fire…\n", v.cfg.ScheduleLead+time.Minute)
	n := v.awaitDue(ctx, since, v.fired, v.cfg.ScheduleLead+time.Minute)
	v.check(n == len(v.fired), "%d of %d schedules fired onto %s", n, len(v.fired), kafka.TopicDue)
}

type wheelStats struct {
	PodIndex    int `json:"pod_index"`
	TotalPods   int `json:"total_pods"`
	TotalLoaded int `json:"total_loaded"`
}

func (v *verifier) wheelStats(ctx context.Context, shard int) (wheelStats, error) {
	var stats wheelStats
	resp, err := v.Request(ctx, http.MethodGet, v.Scheduler(shard)+"/internal/wheel/stats", v.AdminKey, nil)
	if err != nil {
		return stats, err
	}
	if resp.Code != http.StatusOK {
		return stats, fmt.Errorf("wheel stats: %d %s", resp.Code, resp.Body)
	}
	return stats, json.Unmarshal(resp.Body, &stats)
}

// checkDelivery checks the delivery workers sent the batch the scheduler
// fired.
func (v *verifier) checkDelivery(ctx context.Context) {
	v.section("Delivery")
	if len(v.fired) == 0 {
		v.fail("no fired schedules to deliver")
		return
	}
	n := v.awaitStatus(ctx, time.Minute, v.fired, db.ScheduleStatusDelivered)
	v.check(n == len(v.fired), "%d of %d schedules delivered through the mock provider", n, len(v.fired))
	v.checkMetric(ctx, `sum(hatch_delivery_sends_total{status="success"})`, 1)
}

// checkResend sends a real email through Resend: the worker has to decrypt the
// client's API key and call Resend with it.
func (v *verifier) checkResend(ctx context.Context) {
	v.section("Delivery through Resend")
	if v.cfg.ResendAPIKey == "" {
		v.fail("VERIFY_RESEND_API_KEY is not set")
		return
	}
	client, key, err := v.NewClient(ctx, v.runID+"-resend", 50, "resend", map[string]string{"api_key": v.cfg.ResendAPIKey})
	if err != nil {
		v.fail("create a client with a Resend provider: %v", err)
		return
	}
	defer func() { _ = v.DeleteClient(ctx, client) }()

	email := v.email(time.Now().Add(v.cfg.ScheduleLead), v.cfg.ResendTo)
	email["from_email"] = v.cfg.ResendFrom
	id, err := v.createSchedule(ctx, key, email)
	if err != nil {
		v.fail("create a schedule: %v", err)
		return
	}
	if err := v.Poll(ctx); err != nil {
		v.fail("%v", err)
		return
	}
	fmt.Printf("  waiting up to %s for it to be delivered…\n", v.cfg.ScheduleLead+time.Minute)
	n := v.awaitStatus(ctx, v.cfg.ScheduleLead+time.Minute, []uuid.UUID{id}, db.ScheduleStatusDelivered)
	v.check(n == 1, "Resend delivered an email from %s to %s", v.cfg.ResendFrom, v.cfg.ResendTo)
}

// checkRetries checks each retry tier drains back onto emails.due, and that a
// send that keeps failing climbs every tier and then fails for good.
func (v *verifier) checkRetries(ctx context.Context) {
	v.section("Retries")

	// These ids name no schedule, so the delivery workers drop them.
	since := time.Now()
	var probes []uuid.UUID
	var records []*kgo.Record
	for _, tier := range kafka.RetryTiers {
		id := db.NewScheduleID(since)
		probes = append(probes, id)
		records = append(records, kafka.DueRecord(ctx, tier.Topic, id))
	}
	if err := v.producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		v.fail("produce to the retry tiers: %v", err)
		return
	}
	n := v.awaitDue(ctx, since, probes, time.Minute)
	v.check(n == len(probes), "%d of %d retry tiers drained back onto %s", n, len(probes), kafka.TopicDue)
	v.checkMetric(ctx, `sum(hatch_retry_drained_total)`, 1)

	// A client of its own, so its failures do not trip the circuit breaker on
	// the audit's main client.
	client, key, err := v.NewClient(ctx, v.runID+"-retry", 50, "mock", map[string]string{"api_key": "verify"})
	if err != nil {
		v.fail("create a client: %v", err)
		return
	}
	defer func() { _ = v.DeleteClient(ctx, client) }()
	id, err := v.createSchedule(ctx, key, v.email(time.Now().Add(v.cfg.ScheduleLead), v.cfg.FailRecipient))
	if err != nil {
		v.fail("create a schedule: %v", err)
		return
	}
	// Put it straight on emails.due rather than wait for it to come due, the
	// way reconciliation re-enqueues a schedule.
	if err := v.producer.ProduceSync(ctx, kafka.DueRecord(ctx, kafka.TopicDue, id)).FirstErr(); err != nil {
		v.fail("produce to %s: %v", kafka.TopicDue, err)
		return
	}
	v.awaitStatus(ctx, 2*time.Minute, []uuid.UUID{id}, db.ScheduleStatusFailed)
	rows, err := v.schedules(ctx, id)
	if err != nil || len(rows) != 1 {
		v.fail("read schedule %s: %v", id, err)
		return
	}
	row := rows[0]
	reason := value(row.FailureReason)
	v.check(row.Status == db.ScheduleStatusFailed && int(row.RetryCount) == len(kafka.RetryTiers) && strings.HasPrefix(reason, "retry_exhausted"),
		"a send that kept failing is %s after %d retries (%s)", row.Status, row.RetryCount, reason)
}

func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
