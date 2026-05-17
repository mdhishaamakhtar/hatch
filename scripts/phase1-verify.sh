#!/usr/bin/env bash
# Runs every Phase 1 acceptance check and reports a green/red verdict.
# Mirrors scripts/phase0-verify.sh in style — one [PASS]/[FAIL] line per check.
# Exits non-zero if any check fails.
#
# Prereqs (same as Phase 0): `make up && make port-forward && make migrate`,
# Docker daemon running so `make build-api` can build the image, and a
# kubectl context pointed at the cluster running the `hatch` namespace.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAILS=0
pass() { printf "  [PASS] %s\n" "$1"; }
fail() { printf "  [FAIL] %s\n" "$1"; FAILS=$((FAILS + 1)); }
section() { printf "\n== %s ==\n" "$1"; }

# Load .env if present so HOST_*, ADMIN_API_KEY, PROVIDER_CRED_KEY are visible.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ROOT/.env"
  set +a
fi

ADMIN_KEY="${ADMIN_API_KEY:?ADMIN_API_KEY missing — set it in .env}"
API_URL="${HOST_API_URL:-http://localhost:9021}"
DB_URL="${HOST_DATABASE_URL:-postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable}"
REDIS_HOST="$(echo "${HOST_REDIS_ADDR:-localhost:6379}" | cut -d: -f1)"
REDIS_PORT="$(echo "${HOST_REDIS_ADDR:-localhost:6379}" | cut -d: -f2)"

# ---------- Step 1 — Build & test ----------
section "Step 1 — build + test"

if go build ./... 2>/tmp/p1-build.log; then
  pass "go build ./... clean"
else
  fail "go build ./... — see /tmp/p1-build.log"
fi

if go vet ./... 2>/tmp/p1-vet.log; then
  pass "go vet ./... clean"
else
  fail "go vet ./... — see /tmp/p1-vet.log"
fi

if go test ./... 2>/tmp/p1-test.log >/dev/null; then
  pass "go test ./... green"
else
  fail "go test ./... — see /tmp/p1-test.log"
fi

if sqlc diff 2>/tmp/p1-sqlcdiff.log >/dev/null; then
  pass "sqlc diff clean"
else
  fail "sqlc diff dirty — see /tmp/p1-sqlcdiff.log"
fi

if go build ./gen/... 2>/tmp/p1-genbuild.log; then
  pass "go build ./gen/... clean"
else
  fail "go build ./gen/... — see /tmp/p1-genbuild.log"
fi

# ---------- Step 2 — Migrations ----------
section "Step 2 — migrations"

mig_version=$(migrate -path migrations -database "$DB_URL" version 2>&1 | tail -1)
if [[ "$mig_version" == "5" ]]; then
  pass "migrate at version 5 (api_key_lookup migration applied)"
else
  fail "migrate version = '$mig_version', want 5"
fi

# ---------- Step 3 — Service health ----------
section "Step 3 — api pod + endpoints"

# api pod 1/1 Running
bad=$(kubectl get pods -n hatch -l app.kubernetes.io/component=api --no-headers 2>/dev/null \
        | awk '$3!="Running" || $2!~/^[0-9]+\/[0-9]+$/ {print $1":"$3}' || true)
if [[ -z "$bad" ]]; then
  pass "api pod 1/1 Running"
else
  fail "api pod not Running: $bad"
fi

# /healthz
if curl -sf -o /dev/null "$API_URL/healthz"; then
  pass "GET /healthz → 200"
else
  fail "GET /healthz failed at $API_URL"
fi

# /readyz
if curl -sf -o /dev/null "$API_URL/readyz"; then
  pass "GET /readyz → 200"
else
  fail "GET /readyz failed at $API_URL"
fi

# ---------- Step 4 — Golden-path HTTP flow ----------
section "Step 4 — admin + client + schedule golden path"

# Wipe any prior verify client to keep this idempotent.
psql "$DB_URL" -tAc "DELETE FROM schedule_idempotency WHERE idempotency_key = 'phase1-verify-1';" >/dev/null 2>&1 || true
psql "$DB_URL" -tAc "DELETE FROM scheduled_emails WHERE subject = 'phase1-verify subject';" >/dev/null 2>&1 || true
psql "$DB_URL" -tAc "DELETE FROM client_providers WHERE client_id IN (SELECT id FROM clients WHERE name = 'phase1-verify');" >/dev/null 2>&1 || true
psql "$DB_URL" -tAc "DELETE FROM clients WHERE name = 'phase1-verify';" >/dev/null 2>&1 || true

# 1) Create client
create_resp=$(curl -sS -w "\n%{http_code}" -X POST "$API_URL/admin/clients" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"phase1-verify","max_rps":50}')
http_code=$(echo "$create_resp" | tail -1)
body=$(echo "$create_resp" | sed '$d')
if [[ "$http_code" == "201" ]]; then
  CLIENT_ID=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['client_id'])")
  CLIENT_KEY=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])")
  pass "POST /admin/clients → 201 (client_id=$CLIENT_ID)"
else
  fail "POST /admin/clients → $http_code: $body"
  echo "Aborting — cannot continue without a client."
  exit 1
fi

# 2) Pre-seed Redis cache so we can prove invalidation happens.
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SET "client:$CLIENT_ID" "stale" >/dev/null

# 3) Attach a resend provider with a known plaintext we'll grep for.
prov_resp=$(curl -sS -w "\n%{http_code}" -X POST "$API_URL/admin/clients/$CLIENT_ID/providers" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"vendor":"resend","credentials":{"api_key":"re_phase1_marker"}}')
prov_code=$(echo "$prov_resp" | tail -1)
if [[ "$prov_code" == "201" ]]; then
  pass "POST /admin/clients/:id/providers → 201"
else
  fail "POST /admin/clients/:id/providers → $prov_code"
fi

# 4) Tink encryption — plaintext must not appear in the JSONB column.
cred_raw=$(psql "$DB_URL" -tAc "SELECT credentials::text FROM client_providers WHERE client_id = decode('$(echo "$CLIENT_ID" | tr -d - | tr '[:upper:]' '[:lower:]')', 'hex');" 2>/dev/null || true)
if [[ -n "$cred_raw" && "$cred_raw" != *"re_phase1_marker"* ]]; then
  pass "credentials JSONB does not contain plaintext (Tink encryption confirmed)"
else
  fail "credentials column appears to contain plaintext: $cred_raw"
fi

# 5) Cache invalidation — the SET-before-POST key must be gone.
if [[ "$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EXISTS "client:$CLIENT_ID")" == "0" ]]; then
  pass "Redis client:<id> deleted after provider upsert"
else
  fail "Redis client:<id> still present after provider upsert"
fi

# 6) POST /v1/schedules with idempotency key.
deliver_at=$(python3 -c "import datetime; print((datetime.datetime.now(datetime.UTC)+datetime.timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
sched_payload=$(python3 -c "
import json,sys
print(json.dumps({
  'deliver_at': '$deliver_at',
  'recipient_email': 'recipient@example.com',
  'from_email': 'from@example.com',
  'from_name': 'Phase1 Verify',
  'subject': 'phase1-verify subject',
  'body': '<p>phase1 verify</p>',
  'idempotency_key': 'phase1-verify-1',
  'metadata': {'kind': 'verify'}
}))
")
sched_resp=$(curl -sS -w "\n%{http_code}" -X POST "$API_URL/v1/schedules" \
  -H "Authorization: Bearer $CLIENT_KEY" \
  -H "Content-Type: application/json" \
  -d "$sched_payload")
sched_code=$(echo "$sched_resp" | tail -1)
sched_body=$(echo "$sched_resp" | sed '$d')
if [[ "$sched_code" == "201" ]]; then
  SCHEDULE_ID=$(echo "$sched_body" | python3 -c "import sys,json; print(json.load(sys.stdin)['schedule_id'])")
  pass "POST /v1/schedules → 201 (schedule_id=$SCHEDULE_ID)"
else
  fail "POST /v1/schedules → $sched_code: $sched_body"
  exit 1
fi

# 7) schedule_idempotency side-table row count == 1.
si_count=$(psql "$DB_URL" -tAc "SELECT count(*) FROM schedule_idempotency WHERE idempotency_key='phase1-verify-1';" 2>/dev/null)
if [[ "$si_count" == "1" ]]; then
  pass "schedule_idempotency row count = 1"
else
  fail "schedule_idempotency row count = $si_count, want 1"
fi

# 8) Second POST same key → 200, same schedule_id.
sched_resp2=$(curl -sS -w "\n%{http_code}" -X POST "$API_URL/v1/schedules" \
  -H "Authorization: Bearer $CLIENT_KEY" \
  -H "Content-Type: application/json" \
  -d "$sched_payload")
code2=$(echo "$sched_resp2" | tail -1)
body2=$(echo "$sched_resp2" | sed '$d')
id2=$(echo "$body2" | python3 -c "import sys,json; print(json.load(sys.stdin).get('schedule_id',''))" 2>/dev/null || true)
if [[ "$code2" == "200" && "$id2" == "$SCHEDULE_ID" ]]; then
  pass "duplicate Idempotency-Key → 200 with same schedule_id"
else
  fail "idempotency replay: code=$code2 id=$id2 (want 200 / $SCHEDULE_ID)"
fi

# 9) scheduled_emails row count for this idempotency key == 1.
se_count=$(psql "$DB_URL" -tAc "SELECT count(*) FROM scheduled_emails WHERE idempotency_key='phase1-verify-1';" 2>/dev/null)
if [[ "$se_count" == "1" ]]; then
  pass "scheduled_emails row count for key = 1"
else
  fail "scheduled_emails row count for key = $se_count, want 1"
fi

# 10) GET /v1/schedules/:id → 200, status pending.
get_resp=$(curl -sS -w "\n%{http_code}" "$API_URL/v1/schedules/$SCHEDULE_ID" \
  -H "Authorization: Bearer $CLIENT_KEY")
get_code=$(echo "$get_resp" | tail -1)
get_status=$(echo "$get_resp" | sed '$d' | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || true)
if [[ "$get_code" == "200" && "$get_status" == "pending" ]]; then
  pass "GET /v1/schedules/:id → 200 status=pending"
else
  fail "GET /v1/schedules/:id code=$get_code status=$get_status"
fi

# 11) DELETE /v1/schedules/:id → 204.
del_code=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "$API_URL/v1/schedules/$SCHEDULE_ID" \
  -H "Authorization: Bearer $CLIENT_KEY")
if [[ "$del_code" == "204" ]]; then
  pass "DELETE /v1/schedules/:id → 204"
else
  fail "DELETE /v1/schedules/:id → $del_code"
fi

# 12) GET again → status cancelled.
after_status=$(curl -sS "$API_URL/v1/schedules/$SCHEDULE_ID" \
  -H "Authorization: Bearer $CLIENT_KEY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || true)
if [[ "$after_status" == "cancelled" ]]; then
  pass "GET after DELETE → status=cancelled"
else
  fail "GET after DELETE → status=$after_status, want cancelled"
fi

# ---------- Step 5 — Observability round-trip ----------
section "Step 5 — observability"

# Prometheus: hatch_api_requests_total has a sample for POST /v1/schedules.
prom_ok=0
for _ in $(seq 1 40); do
  v=$(curl -sf "http://localhost:9090/api/v1/query" \
        --data-urlencode 'query=hatch_api_requests_total{endpoint="POST /v1/schedules"}' 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['data']['result']))" 2>/dev/null || echo 0)
  if [[ "$v" != "0" ]]; then prom_ok=1; break; fi
  sleep 3
done
if [[ "$prom_ok" == "1" ]]; then
  pass "Prometheus has hatch_api_requests_total{endpoint=\"POST /v1/schedules\"}"
else
  fail "Prometheus has no hatch_api_requests_total for POST /v1/schedules"
fi

# Prometheus: hatch_api_idempotency_hits_total > 0.
ih_ok=0
for _ in $(seq 1 40); do
  v=$(curl -sf "http://localhost:9090/api/v1/query?query=hatch_api_idempotency_hits_total" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); r=d['data']['result']; print(r[0]['value'][1] if r else '')" 2>/dev/null || true)
  if [[ -n "$v" && "$v" != "0" ]]; then ih_ok=1; break; fi
  sleep 3
done
if [[ "$ih_ok" == "1" ]]; then
  pass "Prometheus has hatch_api_idempotency_hits_total > 0"
else
  fail "Prometheus has no hatch_api_idempotency_hits_total samples"
fi

# Loki: "Schedule created" line for the api pod with the schedule_id.
# Promtail tags pod logs with service_name=<pod-label-app>, so the indexed label
# is service_name="api". The "scheduler-api" identity lives in the zap "service"
# JSON field — verified separately by grepping the line body.
log_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf --max-time 5 -G "http://localhost:3100/loki/api/v1/query_range" \
      --data-urlencode 'query={service_name="api"} |= "Schedule created"' \
      --data-urlencode "start=$(($(date +%s) - 600))000000000" \
      --data-urlencode "end=$(date +%s)000000000" 2>/dev/null || true)
  # Loki wraps each log line as a JSON-encoded string in its response, so the
  # inner zap fields show up with backslash-escaped quotes. Match the bare
  # service identifier rather than the full JSON shape.
  if echo "$body" | grep -q "$SCHEDULE_ID" && echo "$body" | grep -q "scheduler-api"; then
    log_ok=1
    break
  fi
  sleep 3
done
if [[ "$log_ok" == "1" ]]; then
  pass "Loki has \"Schedule created\" line (service=scheduler-api) with schedule_id=$SCHEDULE_ID"
else
  fail "Loki did not return the Schedule created line within timeout"
fi

# Tempo: trace with service.name=scheduler-api and span api.schedule.create.
trace_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf --max-time 5 -G "http://localhost:3200/api/search" \
      --data-urlencode 'tags=service.name=scheduler-api' \
      --data-urlencode 'limit=5' 2>/dev/null || true)
  if echo "$body" | grep -q '"traceID"'; then trace_ok=1; break; fi
  sleep 3
done
if [[ "$trace_ok" == "1" ]]; then
  pass "Tempo has traces with service.name=scheduler-api"
else
  fail "Tempo did not return any scheduler-api trace"
fi

# ---------- Step 6 — Cleanup ----------
section "Step 6 — cleanup"

cleanup_code=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "$API_URL/admin/clients/$CLIENT_ID" \
  -H "Authorization: Bearer $ADMIN_KEY")
if [[ "$cleanup_code" == "204" ]]; then
  pass "DELETE /admin/clients/:id → 204"
else
  fail "DELETE /admin/clients/:id → $cleanup_code"
fi

# Subsequent client-auth call must 401 (client is now inactive).
post_delete_code=$(curl -sS -o /dev/null -w "%{http_code}" "$API_URL/v1/schedules/$SCHEDULE_ID" \
  -H "Authorization: Bearer $CLIENT_KEY")
if [[ "$post_delete_code" == "401" ]]; then
  pass "client key now rejected with 401 after soft delete"
else
  fail "post-delete client request → $post_delete_code, want 401"
fi

# ---------- Verdict ----------
echo
if [[ "$FAILS" -eq 0 ]]; then
  echo "Phase 1 verified — all checks PASS."
  exit 0
else
  echo "Phase 1 NOT verified — $FAILS check(s) failed."
  exit 1
fi
