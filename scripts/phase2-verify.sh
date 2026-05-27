#!/usr/bin/env bash
# Runs every Phase 2 acceptance check and reports a green/red verdict.
# Mirrors scripts/phase1-verify.sh in style.
#
# All scheduled emails are created via POST /v1/schedules so the chain is
# proven end-to-end (API → Postgres → Scheduler → Kafka). Kafka consumption
# uses a brand-new throwaway consumer group (group.id = $RUN_ID) with
# auto.offset.reset=earliest and enable.auto.commit=false, then deletes the
# group on exit — every run starts clean and isn't flaky on prior offsets.
#
# Prereqs (same as Phase 1) plus a deployed scheduler StatefulSet:
#   make build-api build-scheduler && make up && make port-forward && make migrate
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAILS=0
pass() { printf "  [PASS] %s\n" "$1"; }
fail() { printf "  [FAIL] %s\n" "$1"; FAILS=$((FAILS + 1)); }
section() { printf "\n== %s ==\n" "$1"; }

# Load .env if present.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ROOT/.env"
  set +a
fi

ADMIN_KEY="${ADMIN_API_KEY:?ADMIN_API_KEY missing — set it in .env}"
API_URL="${HOST_API_URL:-http://localhost:9021}"
DB_URL="${HOST_DATABASE_URL:-postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable}"
SCHED_0="${HOST_SCHEDULER_0:-http://localhost:9022}"
SCHED_1="${HOST_SCHEDULER_1:-http://localhost:9023}"

RUN_ID="phase2-$(uuidgen | tr '[:upper:]' '[:lower:]')"
echo "RUN_ID=$RUN_ID"

cleanup() {
  # Always delete the throwaway consumer group so we never leak ephemera.
  kubectl -n hatch exec -i kafka-0 -- \
    /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --delete --group "$RUN_ID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------- Step 1 — Build + test ----------
section "Step 1 — build + test"

if go build ./... 2>/tmp/p2-build.log; then
  pass "go build ./... clean"
else
  fail "go build ./... — see /tmp/p2-build.log"
fi

if go vet ./... 2>/tmp/p2-vet.log; then
  pass "go vet ./... clean"
else
  fail "go vet ./... — see /tmp/p2-vet.log"
fi

if go test -race ./pkg/... ./internal/... 2>/tmp/p2-test.log >/dev/null; then
  pass "go test -race ./... green"
else
  fail "go test -race ./... — see /tmp/p2-test.log"
fi

# ---------- Step 2 — Kafka topic ----------
section "Step 2 — emails.due topic"

topic_info=$(kubectl -n hatch exec kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic emails.due 2>/dev/null || true)
if echo "$topic_info" | grep -q "Topic: emails.due"; then
  parts=$(echo "$topic_info" | head -1 | grep -oE "PartitionCount: [0-9]+" | awk '{print $2}')
  if [[ "$parts" == "12" ]]; then
    pass "emails.due exists with 12 partitions"
  else
    fail "emails.due partition count = $parts, want 12"
  fi
else
  fail "emails.due topic missing"
fi

# ---------- Step 3 — Pods + admin endpoints ----------
section "Step 3 — scheduler pods + admin endpoints"

bad=$(kubectl get pods -n hatch -l app.kubernetes.io/component=scheduler --no-headers 2>/dev/null \
        | awk '$3!="Running" || $2!~/^[0-9]+\/[0-9]+$/ {print $1":"$3}' || true)
if [[ -z "$bad" ]]; then
  pass "scheduler pods all Running"
else
  fail "scheduler pods not Running: $bad"
fi

# Per-pod admin port-forwards are owned by `make port-forward` (scheduler-0
# on 9022, scheduler-1 on 9023). Re-running this script does not need to
# spin up its own forwards.
stats_0=$(curl -sS -H "Authorization: Bearer $ADMIN_KEY" "$SCHED_0/internal/wheel/stats" || true)
stats_1=$(curl -sS -H "Authorization: Bearer $ADMIN_KEY" "$SCHED_1/internal/wheel/stats" || true)

if echo "$stats_0" | grep -q '"pod_index":0' && echo "$stats_0" | grep -q '"total_pods":2'; then
  pass "scheduler-0 stats reports pod_index=0 total_pods=2"
else
  fail "scheduler-0 stats wrong: $stats_0"
fi

if echo "$stats_1" | grep -q '"pod_index":1' && echo "$stats_1" | grep -q '"total_pods":2'; then
  pass "scheduler-1 stats reports pod_index=1 total_pods=2"
else
  fail "scheduler-1 stats wrong: $stats_1"
fi

# ---------- Step 4 — Provision API client + provider ----------
section "Step 4 — provision verify client + provider"

psql "$DB_URL" -tAc "DELETE FROM client_providers WHERE client_id IN (SELECT id FROM clients WHERE name = '$RUN_ID');" >/dev/null 2>&1 || true
psql "$DB_URL" -tAc "DELETE FROM clients WHERE name = '$RUN_ID';" >/dev/null 2>&1 || true

create_resp=$(curl -sS -w "\n%{http_code}" -X POST "$API_URL/admin/clients" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$RUN_ID\",\"max_rps\":50}")
http_code=$(echo "$create_resp" | tail -1)
body=$(echo "$create_resp" | sed '$d')
if [[ "$http_code" == "201" ]]; then
  CLIENT_ID=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['client_id'])")
  CLIENT_KEY=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])")
  pass "POST /admin/clients → 201"
else
  fail "POST /admin/clients → $http_code: $body"
  exit 1
fi

prov_code=$(curl -sS -o /dev/null -w "%{http_code}" -X POST "$API_URL/admin/clients/$CLIENT_ID/providers" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"vendor":"resend","credentials":{"api_key":"re_phase2_marker"}}')
if [[ "$prov_code" == "201" ]]; then
  pass "POST /admin/clients/:id/providers → 201"
else
  fail "POST /admin/clients/:id/providers → $prov_code"
fi

# ---------- Step 5 — Golden path: schedule via API on both shards ----------
section "Step 5 — schedule emails via /v1/schedules (both shards)"

EXPECTED_FILE=$(mktemp)
trap 'cleanup; rm -f "$EXPECTED_FILE"' EXIT

# Post all 20 schedules first. deliver_at = now+150s (2.5 min) gives a wide
# window: the rollout restart takes ~60s, endpoint wait ~15s, leaving ~225s
# between pods-ready and actual firing — plenty of time for both shards to
# appear in total_loaded before the slot drains.
# 150s is safely past the 2m API_MIN_SCHEDULE_HORIZON and well within the
# PollHourWindow 1h ceiling.
POSTED=0
deliver_at=$(python3 -c "import datetime; print(int((datetime.datetime.now(datetime.UTC)+datetime.timedelta(seconds=150)).timestamp()*1000))")
for i in $(seq 1 20); do
  payload=$(python3 -c "
import json
print(json.dumps({
  'deliver_at': $deliver_at,
  'recipient_email': 'recipient@example.com',
  'from_email': 'from@example.com',
  'from_name': 'Phase2 Verify',
  'subject': '$RUN_ID',
  'body': '<p>$RUN_ID</p>',
  'idempotency_key': '$RUN_ID-$i',
  'metadata': {'run_id': '$RUN_ID'}
}))")
  resp=$(curl -sS "$API_URL/v1/schedules" \
    -H "Authorization: Bearer $CLIENT_KEY" \
    -H "Content-Type: application/json" \
    -d "$payload")
  sid=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('schedule_id',''))" 2>/dev/null || true)
  if [[ -z "$sid" ]]; then
    fail "schedule create returned no id: $resp"
    continue
  fi
  echo "$sid" >> "$EXPECTED_FILE"
  POSTED=$((POSTED + 1))
done
echo "  posted $POSTED schedules (deliver_at = now+150s)"

# Restart the scheduler StatefulSet so the fresh startup-poll (fires
# immediately on boot) picks up the rows we just created. Without this the
# hourly poller won't see them until the next cycle (~55m from now).
echo "  restarting scheduler pods for fresh startup-poll…"
kubectl -n hatch rollout restart statefulset/scheduler >/dev/null
kubectl -n hatch rollout status statefulset/scheduler --timeout=120s >/dev/null
echo "  scheduler rollout complete"

# Re-establish per-pod port-forwards — kubectl forwards to a specific pod UID
# so they break on restart. Kill the old ones and start fresh.
pkill -f "kubectl.*port-forward.*scheduler" 2>/dev/null || true
sleep 1
kubectl -n hatch port-forward pod/scheduler-0 9022:9022 >/tmp/hatch-pf/scheduler-0.log 2>&1 &
kubectl -n hatch port-forward pod/scheduler-1 9023:9022 >/tmp/hatch-pf/scheduler-1.log 2>&1 &
sleep 3

# Now poll until both shards have loaded ≥1 schedule (the startup-poll should
# have already done so — this loop is just a brief wait for propagation).
GOT_SHARD_0=0
GOT_SHARD_1=0
for _ in $(seq 1 20); do
  s0=$(curl -sS -H "Authorization: Bearer $ADMIN_KEY" "$SCHED_0/internal/wheel/stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_loaded'])" 2>/dev/null || echo 0)
  s1=$(curl -sS -H "Authorization: Bearer $ADMIN_KEY" "$SCHED_1/internal/wheel/stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_loaded'])" 2>/dev/null || echo 0)
  [[ "$s0" -gt 0 ]] && GOT_SHARD_0=1
  [[ "$s1" -gt 0 ]] && GOT_SHARD_1=1
  if [[ "$GOT_SHARD_0" == "1" && "$GOT_SHARD_1" == "1" ]]; then break; fi
  sleep 2
done

if [[ "$GOT_SHARD_0" == "1" && "$GOT_SHARD_1" == "1" ]]; then
  pass "both scheduler shards loaded ≥1 schedule (posted=$POSTED)"
else
  fail "could not hit both shards after $POSTED posts (s0=$GOT_SHARD_0 s1=$GOT_SHARD_1)"
fi

EXPECTED_COUNT=$(wc -l < "$EXPECTED_FILE" | tr -d ' ')
echo "expecting $EXPECTED_COUNT schedule_id(s) on emails.due"

# ---------- Step 6 — Kafka consumption (offset-safe) ----------
section "Step 6 — consume emails.due"

# Sleep until 10s past deliver_at so our messages are already in the topic
# before the consumer starts. With messages already present the consumer drains
# history + our batch in seconds, then exits after a short 30s idle timeout
# rather than the 3-minute wait that would occur if it started while the
# wheel slot hasn't fired yet.
now_ms=$(python3 -c "import time; print(int(time.time()*1000))")
wait_ms=$(( deliver_at - now_ms + 10000 ))
if [[ "$wait_ms" -gt 0 ]]; then
  wait_s=$(( (wait_ms + 999) / 1000 ))
  echo "  sleeping ${wait_s}s until deliver_at + 10s buffer…"
  sleep "$wait_s"
fi

# Throwaway consumer group named after RUN_ID. enable.auto.commit=false so we
# never persist offsets and don't depend on the group coordinator's state.
CONSUMER_OUT=$(mktemp)
trap 'cleanup; rm -f "$EXPECTED_FILE" "$CONSUMER_OUT"' EXIT

kubectl -n hatch exec -i kafka-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic emails.due \
    --group "$RUN_ID" \
    --consumer-property auto.offset.reset=earliest \
    --consumer-property enable.auto.commit=false \
    --timeout-ms 30000 \
  >"$CONSUMER_OUT" 2>/dev/null || true

# Intersect consumed schedule_ids with EXPECTED set.
matched=$(python3 - "$EXPECTED_FILE" "$CONSUMER_OUT" <<'PY'
import json, sys
expected = {l.strip() for l in open(sys.argv[1]) if l.strip()}
seen = set()
for line in open(sys.argv[2]):
    line = line.strip()
    if not line: continue
    try:
        sid = json.loads(line).get("schedule_id", "")
    except Exception:
        continue
    if sid in expected:
        seen.add(sid)
print(len(seen), len(expected))
PY
)
read seen total <<< "$matched"
if [[ "$seen" == "$total" && "$total" -gt 0 ]]; then
  pass "consumed all $total expected schedule_ids on emails.due"
else
  fail "consumed $seen of $total expected schedule_ids (run id $RUN_ID)"
fi

# ---------- Step 7 — Observability ----------
section "Step 7 — observability"

# Loki: "wheel slot fired" line on the scheduler pod's stream.
# Phase 1 quirks that apply here too:
#   1) Promtail's *indexed* label is `service_name=<k8s component label>`, so
#      the scheduler stream is service_name="scheduler" — NOT the zap
#      "service":"scheduler-service" field, which lives inside the log body.
#   2) Loki wraps each line as a JSON-encoded string in the API response, so
#      inner JSON shows backslash-escaped quotes. Match the bare token
#      ("scheduler-service") rather than the full \"service\":\"…\" shape.
log_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf --max-time 5 -G "http://localhost:3100/loki/api/v1/query_range" \
      --data-urlencode 'query={service_name="scheduler"} |= "wheel slot fired"' \
      --data-urlencode "start=$(($(date +%s) - 600))000000000" \
      --data-urlencode "end=$(date +%s)000000000" 2>/dev/null || true)
  if echo "$body" | grep -q "scheduler-service"; then
    log_ok=1
    break
  fi
  sleep 3
done
if [[ "$log_ok" == "1" ]]; then
  pass "Loki has \"wheel slot fired\" line (service=scheduler-service)"
else
  fail "Loki did not return wheel slot fired lines"
fi

# Tempo: trace with service.name=scheduler-service.
trace_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf --max-time 5 -G "http://localhost:3200/api/search" \
      --data-urlencode 'tags=service.name=scheduler-service' \
      --data-urlencode 'limit=5' 2>/dev/null || true)
  if echo "$body" | grep -q '"traceID"'; then trace_ok=1; break; fi
  sleep 3
done
if [[ "$trace_ok" == "1" ]]; then
  pass "Tempo has traces with service.name=scheduler-service"
else
  fail "Tempo did not return any scheduler-service trace"
fi

# Prometheus: hatch_scheduler_poll_emails_loaded_total has data for each pod.
prom_ok=0
for _ in $(seq 1 40); do
  v=$(curl -sf "http://localhost:9090/api/v1/query" \
        --data-urlencode 'query=sum by (pod_index) (hatch_scheduler_poll_emails_loaded_total)' 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['data']['result']))" 2>/dev/null || echo 0)
  if [[ "$v" -ge 2 ]]; then prom_ok=1; break; fi
  sleep 3
done
if [[ "$prom_ok" == "1" ]]; then
  pass "Prometheus has hatch_scheduler_poll_emails_loaded_total for ≥2 pod_index labels"
else
  fail "Prometheus missing poll_emails_loaded samples per pod"
fi

# No ERROR logs in either scheduler pod over the last 5 minutes.
err_lines=$(kubectl -n hatch logs -l app.kubernetes.io/component=scheduler --since=5m 2>/dev/null \
              | grep -c '"level":"error"' || true)
if [[ "$err_lines" == "0" ]]; then
  pass "no ERROR logs in scheduler pods in the last 5m"
else
  fail "$err_lines ERROR log line(s) in scheduler pods"
fi

# ---------- Step 8 — Cleanup verify client ----------
section "Step 8 — cleanup"

del_code=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "$API_URL/admin/clients/$CLIENT_ID" \
  -H "Authorization: Bearer $ADMIN_KEY")
if [[ "$del_code" == "204" ]]; then
  pass "DELETE /admin/clients/:id → 204"
else
  fail "DELETE /admin/clients/:id → $del_code"
fi

# ---------- Verdict ----------
echo
if [[ "$FAILS" -eq 0 ]]; then
  echo "Phase 2 verified — all checks PASS."
  exit 0
else
  echo "Phase 2 NOT verified — $FAILS check(s) failed."
  exit 1
fi
