#!/usr/bin/env bash
# Checks the code, then runs the acceptance audit (cmd/verify) as a Job in the
# cluster against the deployed stack, and exits with its result.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAILS=0
check() { # <description> <command...>
  local what="$1"; shift
  if "$@" >/tmp/hatch-verify.log 2>&1; then
    printf "  [PASS] %s\n" "$what"
  else
    printf "  [FAIL] %s\n" "$what"; sed 's/^/    /' /tmp/hatch-verify.log | tail -20
    FAILS=$((FAILS + 1))
  fi
}
not_running() { # <namespace>: lists pods neither running nor completed, bar the audit's own
  kubectl get pods -n "$1" -l 'app.kubernetes.io/component!=verify' --no-headers 2>/dev/null |
    awk '$3 != "Running" && $3 != "Completed" {print $1 ":" $3}'
}

printf "\n== Code ==\n"
check "go build" go build ./...
check "go vet" go vet ./...
check "go test -race" go test -race ./...
check "internal/db matches queries/ and migrations/ (sqlc diff)" sqlc diff

printf "\n== Pods ==\n"
for ns in hatch observability; do
  bad=$(not_running "$ns")
  check "every pod in $ns is running${bad:+; not: $bad}" test -z "$bad"
done

if (( FAILS > 0 )); then
  printf "\n%d check(s) failed; fix them before auditing the cluster.\n" "$FAILS"
  exit 1
fi

printf "\n== Audit ==\n"
if ! make build-verify >/tmp/hatch-verify.log 2>&1; then
  echo "building the verify image failed:" >&2; tail -20 /tmp/hatch-verify.log >&2; exit 1
fi
replicas=$(kubectl -n hatch get statefulset scheduler -o jsonpath='{.spec.replicas}')
kubectl -n hatch delete job hatch-verify --ignore-not-found >/dev/null
sed -e "s|\${VERIFY_IMAGE}|hatch/verify:$(cat .verify-image-tag)|" -e "s|\${SCHEDULER_REPLICAS}|$replicas|" \
  scripts/verify-job.yaml | kubectl apply -f - >/dev/null

# logs -f fails until the container starts, and returns once it exits.
for _ in $(seq 1 180); do
  phase=$(kubectl -n hatch get pod -l app.kubernetes.io/component=verify -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
  [[ "$phase" == Running || "$phase" == Succeeded || "$phase" == Failed ]] && break
  sleep 1
done
kubectl -n hatch logs -f job/hatch-verify 2>/dev/null

for _ in $(seq 1 60); do
  [[ "$(kubectl -n hatch get job hatch-verify -o jsonpath='{.status.succeeded}')" == 1 ]] && exit 0
  [[ -n "$(kubectl -n hatch get job hatch-verify -o jsonpath='{.status.failed}')" ]] && exit 1
  sleep 2
done
exit 1
