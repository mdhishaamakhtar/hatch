#!/usr/bin/env bash
# Hatch acceptance verifier — thin host wrapper around the in-cluster Job.
#
# Host prelude (none of it port-forward-brittle): static Go checks, sqlc diff,
# and a pod-status sweep. Then it builds the verify image, applies the
# hatch-verify Job, streams its [PASS]/[FAIL] report, and exits with the Job's
# result. Everything stateful (DB/Redis/Kafka/API/scheduler/Prometheus/Loki/
# Tempo) is reached by the Job over ClusterDNS — no port-forwards required.
#
# Prereqs: the stack is deployed (`make up`) and migrated (`make migrate`), and
# kubectl points at the cluster running the `hatch` namespace.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAILS=0
pass() { printf "  [PASS] %s\n" "$1"; }
fail() { printf "  [FAIL] %s\n" "$1"; FAILS=$((FAILS + 1)); }
section() { printf "\n== %s ==\n" "$1"; }

# ---------- Host prelude — static checks (no cluster I/O) ----------
section "Host — build, vet, test, sqlc"

if go build ./... 2>/tmp/hatch-verify-build.log; then
  pass "go build ./... clean"
else
  fail "go build ./... — see /tmp/hatch-verify-build.log"
fi

if go vet ./... 2>/tmp/hatch-verify-vet.log; then
  pass "go vet ./... clean"
else
  fail "go vet ./... — see /tmp/hatch-verify-vet.log"
fi

if go test -race ./... 2>/tmp/hatch-verify-test.log >/dev/null; then
  pass "go test -race ./... green"
else
  fail "go test -race ./... — see /tmp/hatch-verify-test.log"
fi

if sqlc diff 2>/tmp/hatch-verify-sqlc.log >/dev/null; then
  pass "sqlc diff clean (gen/ matches queries/ + migrations/)"
else
  fail "sqlc diff dirty — see /tmp/hatch-verify-sqlc.log"
fi

if go build ./gen/... 2>/tmp/hatch-verify-genbuild.log; then
  pass "go build ./gen/... clean"
else
  fail "go build ./gen/... — see /tmp/hatch-verify-genbuild.log"
fi

# ---------- Host prelude — pod status (control-plane, not port-forward) ----------
section "Host — stack pod status"

# Exclude the verify Job's own (possibly stale) pods — they're ephemeral
# tooling, not part of the stack being audited.
bad=$(kubectl get pods -n hatch -l 'app.kubernetes.io/component!=verify' --no-headers 2>/dev/null \
        | awk '$3!="Running" || $2!~/^[0-9]+\/[0-9]+$/ {print $1":"$3}' \
        | grep -v "Completed" || true)
if [[ -z "$bad" ]]; then
  pass "all pods in 'hatch' namespace Running"
else
  fail "non-Running pods in hatch: $bad"
fi

bad=$(kubectl get pods -n observability --no-headers 2>/dev/null \
        | awk '$3!="Running" && $3!="Completed" {print $1":"$3}' || true)
if [[ -z "$bad" ]]; then
  pass "all pods in 'observability' namespace Running"
else
  fail "non-Running pods in observability: $bad"
fi

# Fail fast — no point spinning up the Job if the host prelude is red.
if [[ "$FAILS" -ne 0 ]]; then
  echo
  echo "Host prelude failed ($FAILS check(s)) — fix before running the in-cluster audit."
  exit 1
fi

# ---------- Build the verify image (unique tag per build) ----------
section "Build — verify image"
if ! make build-verify; then
  echo "verify image build failed" >&2
  exit 1
fi
VERIFY_IMAGE="hatch/verify:$(cat "$ROOT/.verify-image-tag")"
echo "→ using $VERIFY_IMAGE"

# ---------- Apply the in-cluster Job and stream its report ----------
section "In-cluster — hatch-verify Job"

kubectl -n hatch delete job hatch-verify --ignore-not-found >/dev/null 2>&1 || true
sed "s|\${VERIFY_IMAGE}|${VERIFY_IMAGE}|g" "$ROOT/scripts/verify-job.yaml" | kubectl apply -f -

# Wait until the container has actually started (left Pending/ContainerCreating)
# before attaching — otherwise `logs -f` errors out immediately. Breaking on any
# non-pending phase also covers a fast-failing pod without burning the timeout.
echo "  waiting for the verify pod to start…"
for _ in $(seq 1 180); do
  phase=$(kubectl -n hatch get pod -l app.kubernetes.io/component=verify \
            -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
  case "$phase" in
    Running | Succeeded | Failed) break ;;
  esac
  sleep 1
done

# Follow the report to completion. The audit runs for a few minutes (it waits
# for schedules to mature on the wheel), so this blocks until the pod ends.
kubectl -n hatch logs -f job/hatch-verify 2>/dev/null || true

# Resolve the Job's terminal status. logs -f only returns once the pod has
# terminated, so the condition settles within a couple of seconds; poll
# generously in case the controller lags.
RESULT=1
for _ in $(seq 1 60); do
  if [[ "$(kubectl -n hatch get job hatch-verify -o jsonpath='{.status.succeeded}' 2>/dev/null)" == "1" ]]; then
    RESULT=0
    break
  fi
  failed=$(kubectl -n hatch get job hatch-verify -o jsonpath='{.status.failed}' 2>/dev/null || true)
  if [[ -n "$failed" && "$failed" != "0" ]]; then
    RESULT=1
    break
  fi
  sleep 2
done

echo
if [[ "$RESULT" -eq 0 ]]; then
  echo "Hatch verified — in-cluster audit PASSED."
else
  echo "Hatch NOT verified — in-cluster audit FAILED (see [FAIL] lines above)."
fi
exit "$RESULT"
