package verify

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// checkDelivery asserts the delivery worker consumed the schedule_ids the
// scheduler fired onto emails.due, sent them through the mock provider, and
// drove every row to status=delivered. It reuses the batch posted by
// checkScheduler (deliver_at = now+lead) and polls Postgres until they settle.
func (v *Verifier) checkDelivery(ctx context.Context) {
	v.rep.Section("Delivery — emails.due → mock provider → delivered")

	if len(v.postedIDs) == 0 {
		v.rep.Fail("no posted schedules available; skipping delivery check")
		return
	}

	idBytes := make([][]byte, 0, len(v.postedIDs))
	for _, s := range v.postedIDs {
		if u, err := uuid.Parse(s); err == nil {
			idBytes = append(idBytes, db.UUIDToBytes(u))
		}
	}

	delivered := retry(ctx, 60, 2*time.Second, func() bool {
		var n int
		err := v.pool.QueryRow(ctx,
			`SELECT count(*) FROM scheduled_emails WHERE id = ANY($1::bytea[]) AND status = 'delivered'`,
			idBytes).Scan(&n)
		return err == nil && n == len(idBytes)
	})
	v.rep.Check(delivered,
		fmt.Sprintf("all %d scheduled emails reached status=delivered (mock provider)", len(idBytes)),
		"not all scheduled emails reached status=delivered — is the delivery-worker running?")

	// Prometheus: the worker recorded successful sends.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		_, val, err := v.promCount(ctx, `sum(hatch_delivery_sends_total{status="success"})`)
		return err == nil && val != "" && val != "0"
	}), `Prometheus has hatch_delivery_sends_total{status="success"} > 0`,
		"Prometheus missing successful delivery sends")
}

// checkResendDelivery performs a live Resend send end-to-end: it provisions a
// dedicated client with the real Resend API key, schedules one email from the
// verified domain to Resend's sandbox recipient, and asserts the worker decrypts
// the key, calls Resend, and marks the row delivered. Always runs — the key must
// be present in hatch-secrets (VERIFY_RESEND_API_KEY).
func (v *Verifier) checkResendDelivery(ctx context.Context) {
	v.rep.Section("Delivery — live Resend send → delivered")

	if v.cfg.ResendAPIKey == "" {
		v.rep.Fail("VERIFY_RESEND_API_KEY not set — add it to .env and re-run `make up` (the audit always exercises a real Resend send)")
		return
	}

	// Dedicated client so its single active provider is resend (deterministic routing).
	resp, err := v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients", v.cfg.AdminKey,
		map[string]any{"name": v.runID + "-resend", "max_rps": 50})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("create resend client → %v (code %d)", err, codeOf(resp, err))
		return
	}
	clientID := jsonField(resp.body, "client_id")
	clientKey := jsonField(resp.body, "api_key")
	if clientID == "" || clientKey == "" {
		v.rep.Failf("create resend client → 201 but missing client_id/api_key: %s", resp.body)
		return
	}
	// Best-effort cleanup of the dedicated client at the end.
	defer func() {
		_, _ = v.do(ctx, http.MethodDelete, v.cfg.APIBase+"/admin/clients/"+clientID, v.cfg.AdminKey, nil)
	}()

	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients/"+clientID+"/providers", v.cfg.AdminKey,
		map[string]any{"vendor": "resend", "credentials": map[string]any{"api_key": v.cfg.ResendAPIKey}})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("attach resend provider → %v (code %d)", err, codeOf(resp, err))
		return
	}

	lead := v.cfg.ScheduleLeadSeconds
	deliverMs := time.Now().Add(time.Duration(lead) * time.Second).UnixMilli()
	payload := map[string]any{
		"deliver_at":      deliverMs,
		"recipient_email": v.cfg.ResendTo,
		"from_email":      v.cfg.ResendFrom,
		"from_name":       "Hatch Verify",
		"subject":         "Hatch Resend verification " + v.runID,
		"body":            "<p>Hatch live Resend verification " + v.runID + "</p>",
		"idempotency_key": v.runID + "-resend",
	}
	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/v1/schedules", clientKey, payload)
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("POST resend schedule → %v (code %d): %s", err, codeOf(resp, err), bodyOf(resp))
		return
	}
	schedID := jsonField(resp.body, "schedule_id")
	v.rep.Passf("POST resend schedule → 201 (schedule_id=%s, %s → %s)", schedID, v.cfg.ResendFrom, v.cfg.ResendTo)

	// Trigger an out-of-band poll on every shard so the wheel loads the row now.
	for i := 0; i < v.cfg.SchedReplicas; i++ {
		_, _ = v.do(ctx, http.MethodPost, v.cfg.SchedulerURL(i)+"/internal/poll", v.cfg.AdminKey, nil)
	}

	u, err := uuid.Parse(schedID)
	if err != nil {
		v.rep.Failf("resend schedule_id not a uuid: %q", schedID)
		return
	}
	idb := db.UUIDToBytes(u)

	// Poll until the row matures on the wheel and the worker completes the live
	// send (≈ lead seconds + send + propagation).
	delivered := retry(ctx, 120, 2*time.Second, func() bool {
		var n int
		err := v.pool.QueryRow(ctx,
			`SELECT count(*) FROM scheduled_emails WHERE id = $1 AND status = 'delivered'`, idb).Scan(&n)
		return err == nil && n == 1
	})
	v.rep.Check(delivered,
		"live Resend send reached status=delivered",
		"resend schedule did not reach delivered — check VERIFY_RESEND_API_KEY and that "+v.cfg.ResendFrom+"'s domain is verified in Resend")
}
