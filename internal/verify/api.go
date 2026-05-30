package verify

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// checkAPIGoldenPath exercises the full Phase 1 lifecycle against the API over
// ClusterDNS: client + provider creation (with Tink encryption and cache
// invalidation proofs), then a single schedule through create → idempotent
// replay → get → cancel. It leaves the verify client in place; cleanup()
// removes it at the end of the run.
func (v *Verifier) checkAPIGoldenPath(ctx context.Context) {
	v.rep.Section("API — health + golden path")

	// Health endpoints.
	if resp, err := v.do(ctx, http.MethodGet, v.cfg.APIBase+"/healthz", "", nil); err == nil {
		v.rep.Check(resp.code == http.StatusOK, "GET /healthz → 200", fmt.Sprintf("GET /healthz → %d", resp.code))
	} else {
		v.rep.Failf("GET /healthz: %v", err)
	}
	if resp, err := v.do(ctx, http.MethodGet, v.cfg.APIBase+"/readyz", "", nil); err == nil {
		v.rep.Check(resp.code == http.StatusOK, "GET /readyz → 200", fmt.Sprintf("GET /readyz → %d", resp.code))
	} else {
		v.rep.Failf("GET /readyz: %v", err)
	}

	// Create the verify client.
	resp, err := v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients", v.cfg.AdminKey,
		map[string]any{"name": v.runID, "max_rps": 50})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("POST /admin/clients → %v (code %d)", err, codeOf(resp, err))
		return // nothing else in this section can run without a client
	}
	v.clientID = jsonField(resp.body, "client_id")
	v.clientKey = jsonField(resp.body, "api_key")
	if v.clientID == "" || v.clientKey == "" {
		v.rep.Failf("POST /admin/clients → 201 but missing client_id/api_key: %s", resp.body)
		return
	}
	v.rep.Passf("POST /admin/clients → 201 (client_id=%s)", v.clientID)

	// Pre-seed the per-client cache so we can prove the provider upsert
	// invalidates it.
	cacheKey := "client:" + v.clientID
	if err := v.rc.Do(ctx, v.rc.B().Set().Key(cacheKey).Value("stale").Build()).Error(); err != nil {
		v.rep.Failf("seed redis cache key: %v", err)
	}

	// Attach a mock provider whose plaintext we grep for in the DB. The verify
	// client routes through mock so its scheduled emails deliver deterministically
	// and offline (the live Resend leg is exercised separately in checkResendDelivery).
	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/admin/clients/"+v.clientID+"/providers", v.cfg.AdminKey,
		map[string]any{"vendor": "mock", "credentials": map[string]any{"api_key": v.marker}})
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("POST /admin/clients/:id/providers → %v (code %d)", err, codeOf(resp, err))
	} else {
		v.rep.Pass("POST /admin/clients/:id/providers → 201")
	}

	// Tink encryption: the plaintext marker must not appear in the JSONB column.
	if id, perr := uuid.Parse(v.clientID); perr == nil {
		var credText string
		qerr := v.pool.QueryRow(ctx,
			`SELECT credentials::text FROM client_providers WHERE client_id = $1`,
			db.UUIDToBytes(id)).Scan(&credText)
		switch {
		case qerr != nil:
			v.rep.Failf("read client_providers.credentials: %v", qerr)
		case strings.Contains(credText, v.marker):
			v.rep.Fail("credentials column contains plaintext marker (Tink encryption NOT confirmed)")
		default:
			v.rep.Pass("credentials JSONB does not contain plaintext (Tink encryption confirmed)")
		}
	}

	// Cache invalidation: the pre-seeded key must be gone after the upsert.
	existed, err := v.rc.Do(ctx, v.rc.B().Exists().Key(cacheKey).Build()).AsInt64()
	if err != nil {
		v.rep.Failf("EXISTS redis cache key: %v", err)
	} else {
		v.rep.Check(existed == 0,
			"Redis client:<id> deleted after provider upsert",
			"Redis client:<id> still present after provider upsert")
	}

	// Create a schedule far in the future (won't fire) for the lifecycle checks.
	idemKey := v.runID + "-golden"
	payload := v.schedulePayload(time.Now().Add(2*time.Hour).UnixMilli(), idemKey)
	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/v1/schedules", v.clientKey, payload)
	if err != nil || resp.code != http.StatusCreated {
		v.rep.Failf("POST /v1/schedules → %v (code %d): %s", err, codeOf(resp, err), bodyOf(resp))
		return
	}
	v.schedID = jsonField(resp.body, "schedule_id")
	v.rep.Passf("POST /v1/schedules → 201 (schedule_id=%s)", v.schedID)

	// schedule_idempotency side-table row count == 1.
	v.rep.Check(v.countRows(ctx, "schedule_idempotency", idemKey) == 1,
		"schedule_idempotency row count = 1",
		"schedule_idempotency row count != 1")

	// Replaying the same key → 200 with the same schedule_id.
	resp, err = v.do(ctx, http.MethodPost, v.cfg.APIBase+"/v1/schedules", v.clientKey, payload)
	replayID := ""
	if err == nil {
		replayID = jsonField(resp.body, "schedule_id")
	}
	v.rep.Check(err == nil && resp.code == http.StatusOK && replayID == v.schedID,
		"duplicate idempotency key → 200 with same schedule_id",
		fmt.Sprintf("idempotency replay: code=%d id=%s (want 200 / %s)", codeOf(resp, err), replayID, v.schedID))

	// scheduled_emails row count for this key == 1.
	v.rep.Check(v.countRows(ctx, "scheduled_emails", idemKey) == 1,
		"scheduled_emails row count for key = 1",
		"scheduled_emails row count for key != 1")

	// GET → 200 status=pending.
	resp, err = v.do(ctx, http.MethodGet, v.cfg.APIBase+"/v1/schedules/"+v.schedID, v.clientKey, nil)
	v.rep.Check(err == nil && resp.code == http.StatusOK && jsonField(resp.body, "status") == "pending",
		"GET /v1/schedules/:id → 200 status=pending",
		fmt.Sprintf("GET /v1/schedules/:id code=%d status=%s", codeOf(resp, err), statusOf(resp)))

	// DELETE → 204.
	resp, err = v.do(ctx, http.MethodDelete, v.cfg.APIBase+"/v1/schedules/"+v.schedID, v.clientKey, nil)
	v.rep.Check(err == nil && resp.code == http.StatusNoContent,
		"DELETE /v1/schedules/:id → 204",
		fmt.Sprintf("DELETE /v1/schedules/:id → %d", codeOf(resp, err)))

	// GET again → status cancelled.
	resp, err = v.do(ctx, http.MethodGet, v.cfg.APIBase+"/v1/schedules/"+v.schedID, v.clientKey, nil)
	v.rep.Check(err == nil && jsonField(resp.body, "status") == "cancelled",
		"GET after DELETE → status=cancelled",
		fmt.Sprintf("GET after DELETE → status=%s, want cancelled", statusOf(resp)))
}

// schedulePayload builds a /v1/schedules request body tagged with the run id.
func (v *Verifier) schedulePayload(deliverAtMs int64, idemKey string) map[string]any {
	return map[string]any{
		"deliver_at":      deliverAtMs,
		"recipient_email": "recipient@example.com",
		"from_email":      "from@example.com",
		"from_name":       "Hatch Verify",
		"subject":         v.runID,
		"body":            "<p>" + v.runID + "</p>",
		"idempotency_key": idemKey,
		"metadata":        map[string]any{"run_id": v.runID},
	}
}

// countRows returns the row count in table for the given idempotency_key.
func (v *Verifier) countRows(ctx context.Context, table, idemKey string) int {
	var n int
	// table is a fixed internal literal, never user input.
	if err := v.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s WHERE idempotency_key = $1", table), idemKey).Scan(&n); err != nil {
		return -1
	}
	return n
}

func codeOf(r httpResp, err error) int {
	if err != nil {
		return 0
	}
	return r.code
}

func bodyOf(r httpResp) string   { return string(r.body) }
func statusOf(r httpResp) string { return jsonField(r.body, "status") }
