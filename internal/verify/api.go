package verify

import (
	"bytes"
	"context"
	"net/http"
	"time"
	"uuid"
)

// checkAPI creates the client the rest of the audit schedules as, and takes one
// schedule through its lifecycle: create, replay, read, cancel.
func (v *verifier) checkAPI(ctx context.Context) {
	v.section("API")

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := v.API(ctx, http.MethodGet, path, "", nil)
		v.expect("GET "+path, resp, err, http.StatusOK)
	}

	// The provider's credentials carry a marker, which must not reach the
	// database in the clear.
	marker := "marker-" + uuid.New().String()
	creds := map[string]string{"api_key": marker}
	id, key, err := v.NewClient(ctx, v.runID, 50, "mock", creds)
	if err != nil {
		v.fail("create a client: %v", err)
		return
	}
	v.client, v.clientKey = id, key
	v.check(true, "created client %s with a mock provider", id)

	var sealed []byte
	if err := v.DB.QueryRow(ctx, `SELECT credentials FROM client_providers WHERE client_id = $1`, v.client[:]).Scan(&sealed); err != nil {
		v.fail("read the provider's credentials: %v", err)
	} else {
		v.check(!bytes.Contains(sealed, []byte(marker)), "provider credentials are stored encrypted")
	}

	// Delivery workers cache a client's providers, so registering one has to
	// evict that entry.
	cacheKey := "client:" + id.String()
	if err := v.redis.Do(ctx, v.redis.B().Set().Key(cacheKey).Value("stale").Build()).Error(); err != nil {
		v.fail("seed the client's cache entry: %v", err)
	}
	resp, err := v.API(ctx, http.MethodPost, "/admin/clients/"+id.String()+"/providers", v.AdminKey,
		map[string]any{"vendor": "mock", "credentials": creds})
	v.expect("POST /admin/clients/{id}/providers", resp, err, http.StatusCreated)
	cached, err := v.redis.Do(ctx, v.redis.B().Exists().Key(cacheKey).Build()).AsInt64()
	v.check(err == nil && cached == 0, "registering a provider evicted the client's cache entry")

	// Due in two hours, so it never fires.
	email := v.email(time.Now().Add(2*time.Hour), "recipient@example.com")
	email["idempotency_key"] = v.runID
	if v.golden, err = v.createSchedule(ctx, key, email); err != nil {
		v.fail("POST /v1/schedules: %v", err)
		return
	}
	v.check(true, "POST /v1/schedules created schedule %s", v.golden)

	resp, err = v.API(ctx, http.MethodPost, "/v1/schedules", key, email)
	if v.expect("POST /v1/schedules with the same idempotency key", resp, err, http.StatusOK) {
		v.check(resp.Field("schedule_id") == v.golden.String(), "the replay answered with schedule %s", resp.Field("schedule_id"))
	}
	var n int
	err = v.DB.QueryRow(ctx, `SELECT count(*) FROM scheduled_emails WHERE client_id = $1 AND idempotency_key = $2`,
		v.client[:], v.runID).Scan(&n)
	v.check(err == nil && n == 1, "the idempotency key created %d schedule(s)", n)

	path := "/v1/schedules/" + v.golden.String()
	resp, err = v.API(ctx, http.MethodGet, path, key, nil)
	if v.expect("GET /v1/schedules/{id}", resp, err, http.StatusOK) {
		v.check(resp.Field("status") == "pending", "the schedule is %s", resp.Field("status"))
	}
	resp, err = v.API(ctx, http.MethodDelete, path, key, nil)
	v.expect("DELETE /v1/schedules/{id}", resp, err, http.StatusNoContent)
	resp, err = v.API(ctx, http.MethodGet, path, key, nil)
	if v.expect("GET /v1/schedules/{id}", resp, err, http.StatusOK) {
		v.check(resp.Field("status") == "cancelled", "the schedule is %s", resp.Field("status"))
	}
	resp, err = v.API(ctx, http.MethodDelete, path, key, nil)
	v.expect("DELETE /v1/schedules/{id} again", resp, err, http.StatusConflict)
}

// checkClientDeletion deletes the audit's client, and checks its key stops
// working.
func (v *verifier) checkClientDeletion(ctx context.Context) {
	v.section("Client deletion")
	if v.clientKey == "" {
		v.fail("no client to delete")
		return
	}
	if err := v.DeleteClient(ctx, v.client); err != nil {
		v.fail("%v", err)
		return
	}
	resp, err := v.API(ctx, http.MethodGet, "/v1/schedules/"+v.golden.String(), v.clientKey, nil)
	v.expect("the deleted client's key", resp, err, http.StatusUnauthorized)
}
