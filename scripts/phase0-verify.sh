#!/usr/bin/env bash
# Runs every Phase 0 acceptance check and reports a green/red verdict.
# Exits 1 if any check fails. Each check prints one line in the form:
#   [PASS] / [FAIL] description (evidence)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAILS=0
PROBES_STARTED=0

pass() { printf "  [PASS] %s\n" "$1"; }
fail() { printf "  [FAIL] %s\n" "$1"; FAILS=$((FAILS + 1)); }
section() { printf "\n== %s ==\n" "$1"; }

cleanup_probes() {
  [[ "$PROBES_STARTED" == "1" ]] || return 0
  kubectl -n hatch delete --ignore-not-found pod metric-probe log-probe >/dev/null 2>&1 || true
  kubectl -n hatch delete --ignore-not-found job trace-probe             >/dev/null 2>&1 || true
  kubectl -n hatch delete --ignore-not-found configmap metric-probe-config >/dev/null 2>&1 || true
}
trap cleanup_probes EXIT

# ---------- Step 1 — Go ----------
section "Step 1 — Go module + shared packages"

if go build ./... 2>/tmp/p0-build.log; then
  pass "go build ./... clean"
else
  fail "go build ./... — see /tmp/p0-build.log"
fi

if go vet ./... 2>/tmp/p0-vet.log; then
  pass "go vet ./... clean"
else
  fail "go vet ./... — see /tmp/p0-vet.log"
fi

if go test ./pkg/... 2>/tmp/p0-test.log >/dev/null; then
  pass "go test ./pkg/... green"
else
  fail "go test ./pkg/... — see /tmp/p0-test.log"
fi

# ---------- Step 2 — infra pods ----------
section "Step 2 — stack health"

bad=$(kubectl get pods -n hatch --no-headers 2>/dev/null | awk '$3!="Running" || $2!~/^[0-9]+\/[0-9]+$/ {print $1":"$3}' | grep -v "Completed" || true)
if [[ -z "$bad" ]]; then
  pass "all pods in 'hatch' namespace Running"
else
  fail "non-Running pods in hatch: $bad"
fi

bad=$(kubectl get pods -n observability --no-headers 2>/dev/null | awk '$3!="Running" && $3!="Completed" {print $1":"$3}' || true)
if [[ -z "$bad" ]]; then
  pass "all pods in 'observability' namespace Running"
else
  fail "non-Running pods in observability: $bad"
fi

# Service reachability via port-forwards (assumed running)
if pg_isready -h localhost -p 5432 -U hatch >/dev/null 2>&1; then
  pass "Postgres reachable on localhost:5432"
else
  fail "Postgres NOT reachable (run 'make port-forward' first)"
fi

if redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q PONG; then
  pass "Redis PONG on localhost:6379"
else
  fail "Redis NOT reachable (run 'make port-forward' first)"
fi

if curl -sf -o /dev/null http://localhost:3000/api/health; then
  pass "Grafana /api/health on :3000"
else
  fail "Grafana not reachable on :3000"
fi

if curl -sf -o /dev/null http://localhost:8080/actuator/health; then
  pass "Kafka UI /actuator/health on :8080"
else
  fail "Kafka UI not reachable on :8080"
fi

if curl -sf -o /dev/null http://localhost:9090/-/healthy; then
  pass "Prometheus /-/healthy on :9090"
else
  fail "Prometheus not reachable on :9090"
fi

if curl -sf -o /dev/null "http://localhost:3100/loki/api/v1/labels"; then
  pass "Loki gateway reachable on :3100"
else
  fail "Loki gateway not reachable on :3100"
fi

if curl -sf -o /dev/null "http://localhost:3200/ready"; then
  pass "Tempo /ready on :3200"
else
  fail "Tempo not reachable on :3200"
fi

# ---------- Probe round-trips ----------
section "Probe round-trips (in-cluster producers)"

# Apply probes (idempotent). Skip if --skip-probes passed.
if [[ "${1:-}" != "--skip-probes" ]]; then
  PROBES_STARTED=1
  if ./scripts/probe/run.sh >/tmp/p0-probes.log 2>&1; then
    pass "probe pods applied + ready"
  else
    fail "probe apply — see /tmp/p0-probes.log"
  fi
fi

# Metric round-trip: ask Prometheus whether hatch_probe_value is being scraped.
metric_ok=0
for _ in $(seq 1 40); do
  v=$(curl -sf "http://localhost:9090/api/v1/query?query=hatch_probe_value" 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); r=d['data']['result']; print(r[0]['value'][1] if r else '')" 2>/dev/null || true)
  if [[ "$v" == "1" ]]; then
    metric_ok=1
    break
  fi
  sleep 3
done
if [[ "$metric_ok" == "1" ]]; then
  pass "Prometheus scraped hatch_probe_value=1 from metric-probe pod"
else
  fail "Prometheus did not return hatch_probe_value within timeout"
fi

# Log round-trip: query Loki for the probe log line by pod label.
log_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf -G "http://localhost:3100/loki/api/v1/query_range" \
      --data-urlencode 'query={pod="log-probe"}' \
      --data-urlencode "start=$(($(date +%s) - 600))000000000" \
      --data-urlencode "end=$(date +%s)000000000" 2>/dev/null || true)
  if echo "$body" | grep -q "phase0 log probe"; then
    log_ok=1
    break
  fi
  sleep 3
done
if [[ "$log_ok" == "1" ]]; then
  pass "Loki returned the log-probe line via {pod=\"log-probe\"}"
else
  fail "Loki did not return the log-probe line within timeout"
fi

# Trace round-trip: search Tempo for traces with service.name=probe.
trace_ok=0
for _ in $(seq 1 40); do
  body=$(curl -sf -G "http://localhost:3200/api/search" \
      --data-urlencode 'tags=service.name=probe' \
      --data-urlencode 'limit=1' 2>/dev/null || true)
  if echo "$body" | grep -q '"traceID"'; then
    trace_ok=1
    break
  fi
  sleep 3
done
if [[ "$trace_ok" == "1" ]]; then
  pass "Tempo returned a trace with service.name=probe"
else
  fail "Tempo did not return any probe trace within timeout"
fi

# ---------- Step 3 — migrations ----------
section "Step 3 — migrations"

DB_URL="${HOST_DATABASE_URL:-postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable}"

mig_version=$(migrate -path migrations -database "$DB_URL" version 2>&1 | tail -1)
if [[ "$mig_version" == "5" ]]; then
  pass "migrate at version 5 (all 5 migrations applied)"
else
  fail "migrate version = '$mig_version', want 5"
fi

partitions=$(psql "$DB_URL" -tAc "SELECT count(*) FROM pg_inherits WHERE inhparent = 'scheduled_emails'::regclass" 2>/dev/null || echo 0)
if [[ "$partitions" -eq 1200 ]]; then
  pass "1200 partitions attached to scheduled_emails"
else
  fail "partition count = $partitions, want 1200"
fi

# ---------- Step 4 — sqlc ----------
section "Step 4 — sqlc"

if sqlc diff 2>/tmp/p0-sqlcdiff.log >/dev/null; then
  pass "sqlc diff clean (gen/ matches queries/ + migrations/)"
else
  fail "sqlc diff dirty — see /tmp/p0-sqlcdiff.log"
fi

if go build ./gen/... 2>/tmp/p0-genbuild.log; then
  pass "go build ./gen/... clean"
else
  fail "go build ./gen/... — see /tmp/p0-genbuild.log"
fi

# ---------- Verdict ----------
echo
if [[ "$FAILS" -eq 0 ]]; then
  echo "Phase 0 verified — all checks PASS."
  exit 0
else
  echo "Phase 0 NOT verified — $FAILS check(s) failed."
  exit 1
fi
