package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hkafka "github.com/mdhishaamakhtar/hatch/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Retry-tier topics the delivery worker produces to on transient failure and the
// Phase 4 retry consumers drain.
const (
	retry1MinTopic  = "emails.retry.1min"
	retry5MinTopic  = "emails.retry.5min"
	retry30MinTopic = "emails.retry.30min"
)

// checkRetry validates the Phase 4 retry consumers two ways:
//   - Part A (isolated): a synthetic schedule_id dropped on each tier topic is
//     drained and re-enqueued onto emails.due.
//   - Part B (end-to-end): a real schedule sent to the MockProvider fail sentinel
//     cascades through all three tiers and lands in the terminal `failed` state.
func (v *Verifier) checkRetry(ctx context.Context) {
	v.rep.Section("Retry — tier drain → emails.due, and full failure cascade")

	producer, err := hkafka.NewProducer(v.cfg.Brokers, v.lg)
	if err != nil {
		v.rep.Failf("kafka producer: %v", err)
		return
	}
	defer producer.Close()

	v.checkRetryDrain(ctx, producer)
	v.checkRetryCascade(ctx, producer)
}

// checkRetryDrain (Part A) drops one uniquely tagged synthetic schedule_id on
// each tier topic and asserts each is re-enqueued onto emails.due. The ids need
// not exist in Postgres — the delivery worker simply finds no row and skips,
// while this consumer reads its own copy of emails.due. A from-end consumer is
// positioned before producing so only fresh re-enqueues are observed.
func (v *Verifier) checkRetryDrain(ctx context.Context, producer *kgo.Client) {
	probes := map[string]uuid.UUID{ // topic → synthetic schedule_id
		retry1MinTopic:  uuid.New(),
		retry5MinTopic:  uuid.New(),
		retry30MinTopic: uuid.New(),
	}
	expected := make(map[string]bool, len(probes))
	for _, id := range probes {
		expected[id.String()] = true
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(v.cfg.Brokers...),
		kgo.ConsumeTopics(dueTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		v.rep.Failf("retry-drain consumer: %v", err)
		return
	}
	defer cl.Close()

	// Poll once to establish the end position across all partitions before we
	// produce, so we don't have to drain the topic's history.
	posCtx, posCancel := context.WithTimeout(ctx, 5*time.Second)
	cl.PollFetches(posCtx)
	posCancel()

	for topic, id := range probes {
		// Key is the 16-byte binary UUID — the cross-service contract the delivery
		// worker produces with — not the 36-byte string form.
		rec := &kgo.Record{Topic: topic, Key: db.UUIDToBytes(id), Value: mustDuePayload(id.String())}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			v.rep.Failf("produce synthetic id to %s: %v", topic, err)
			return
		}
	}

	// Drain frequency tops out at the 30min tier's dev interval; give it margin.
	seen := make(map[string]bool, len(expected))
	cctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	for len(seen) < len(expected) {
		fetches := cl.PollFetches(cctx)
		if len(fetches.Errors()) > 0 {
			break // deadline/cancel
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if sid := jsonField(r.Value, "schedule_id"); expected[sid] {
				seen[sid] = true
			}
		})
	}
	v.rep.Check(len(seen) == len(expected),
		fmt.Sprintf("all %d retry tiers drained their synthetic id back onto emails.due", len(expected)),
		fmt.Sprintf("only %d of %d tier ids re-enqueued onto emails.due — are the retry consumers running?", len(seen), len(expected)))

	// Observability: the consumers recorded the drains.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		_, val, err := v.promCount(ctx, `sum(hatch_retry_drained_total)`)
		return err == nil && val != "" && val != "0"
	}), "Prometheus has hatch_retry_drained_total > 0",
		"Prometheus missing retry drain counts")
}

// checkRetryCascade (Part B) provisions a dedicated mock client, schedules an
// email to the fail sentinel, hands the id straight to emails.due (bypassing the
// scheduler wheel lead), and asserts the row cascades through all three tiers to
// the terminal `failed` state with retry_count=3.
func (v *Verifier) checkRetryCascade(ctx context.Context, producer *kgo.Client) {
	// Dedicated client so the sentinel failures don't perturb other clients' breakers.
	resp, err := v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients", v.cfg.AdminKey,
		map[string]any{"name": v.runID + "-retry", "max_rps": 50})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("create retry client → %v (code %d)", err, codeOf(resp, err))
		return
	}
	clientID := jsonField(resp.body, "client_id")
	clientKey := jsonField(resp.body, "api_key")
	if clientID == "" || clientKey == "" {
		v.rep.Failf("create retry client → 201 but missing client_id/api_key: %s", resp.body)
		return
	}
	defer func() {
		_, _ = v.do(ctx, http.MethodDelete, v.cfg.APIBase+"/admin/clients/"+clientID, v.cfg.AdminKey, nil)
	}()

	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients/"+clientID+"/providers", v.cfg.AdminKey,
		map[string]any{"vendor": "mock", "credentials": map[string]any{"api_key": "mock-retry"}})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("attach mock provider → %v (code %d)", err, codeOf(resp, err))
		return
	}

	deliverMs := time.Now().Add(time.Duration(v.cfg.ScheduleLeadSeconds) * time.Second).UnixMilli()
	payload := map[string]any{
		"deliver_at":      deliverMs,
		"recipient_email": v.cfg.RetryFailRecipient,
		"from_email":      "from@example.com",
		"from_name":       "Hatch Verify",
		"subject":         "Hatch retry cascade " + v.runID,
		"body":            "<p>" + v.runID + "</p>",
		"idempotency_key": v.runID + "-retry",
	}
	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/v1/schedules", clientKey, payload)
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("POST retry schedule → %v (code %d): %s", err, codeOf(resp, err), bodyOf(resp))
		return
	}
	schedID := jsonField(resp.body, "schedule_id")
	u, err := uuid.Parse(schedID)
	if err != nil {
		v.rep.Failf("retry schedule_id not a uuid: %q", schedID)
		return
	}
	idb := db.UUIDToBytes(u)

	// Hand the id straight to emails.due so we don't wait out the wheel lead — the
	// same path reconciliation uses. The worker fetches the row regardless of
	// deliver_at, sends to the sentinel, and the failure cascade begins.
	rec := &kgo.Record{Topic: dueTopic, Key: idb, Value: mustDuePayload(schedID)}
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		v.rep.Failf("produce retry schedule to emails.due: %v", err)
		return
	}

	failed := retry(ctx, 40, 2*time.Second, func() bool {
		var status string
		var rc int16
		var reason *string
		err := v.pool.QueryRow(ctx,
			`SELECT status, retry_count, failure_reason FROM scheduled_emails WHERE id = $1`, idb).
			Scan(&status, &rc, &reason)
		return err == nil && status == "failed" && rc == 3 &&
			reason != nil && strings.HasPrefix(*reason, "retry_exhausted")
	})
	v.rep.Check(failed,
		"sentinel schedule cascaded through all 3 tiers to status=failed (retry_count=3, retry_exhausted)",
		"sentinel schedule did not reach failed/retry_count=3 — check the retry consumers and MOCK_PROVIDER_FAIL_RECIPIENT")
}

// mustDuePayload marshals the thin {schedule_id} envelope carried on emails.due
// and the retry tiers.
func mustDuePayload(scheduleID string) []byte {
	b, _ := json.Marshal(map[string]string{"schedule_id": scheduleID})
	return b
}
