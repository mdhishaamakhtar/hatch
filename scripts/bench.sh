#!/usr/bin/env bash
# Hatch benchmark runner — host wrapper around the in-cluster bench Job.
#
# The host side does only what needs a control-plane client: scaling replicas
# and setting worker config between sweep points, then collecting each Job's
# JSON result. Every measurement happens inside the cluster over ClusterDNS, so
# a run is not at the mercy of a port-forward staying up for an hour.
#
# Usage:
#   scripts/bench.sh one <scenario> [count] [workers] [spread] [label]
#   scripts/bench.sh all
#
# `one` drops a single JSON in benchmarks/results/ (gitignored). `all` runs the
# full sweep and writes benchmarks/reference.{md,json}, the committed artifact
# that docs/BENCHMARKS.md interprets.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NS=hatch
RESULTS_DIR="$ROOT/benchmarks"
JSON_BEGIN="---BENCH-RESULT-BEGIN---"
JSON_END="---BENCH-RESULT-END---"

# ---------- helpers ----------

log()  { printf "\n\033[1m%s\033[0m\n" "$*"; }
note() { printf "  %s\n" "$*"; }

build_image() {
  log "Building the bench image"
  make build-bench >/dev/null 2>&1 || { echo "bench image build failed" >&2; exit 1; }
  BENCH_IMAGE="hatch/bench:$(cat "$ROOT/.bench-image-tag")"
  note "using $BENCH_IMAGE"
}

# scale <deployment> <replicas> — waits for the rollout so a measurement never
# starts against a half-scaled deployment.
scale() {
  kubectl -n "$NS" scale "deployment/$1" --replicas="$2" >/dev/null
  kubectl -n "$NS" rollout status "deployment/$1" --timeout=300s >/dev/null
}

# set_env <deployment> <KEY=VALUE...> — an explicit env entry overrides the
# value the Secret supplies via envFrom.
set_env() {
  local dep="$1"; shift
  kubectl -n "$NS" set env "deployment/$dep" "$@" >/dev/null
  kubectl -n "$NS" rollout status "deployment/$dep" --timeout=300s >/dev/null
}

# replica_summary renders the deployed topology as "name=n,..." so each result
# records the shape it was measured against. The Job cannot look this up itself:
# it runs from a distroless image with no kubectl.
replica_summary() {
  local out=""
  for d in api delivery-worker retry-consumer; do
    local n; n=$(kubectl -n "$NS" get deployment "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
    out="${out}${d}=${n:-0},"
  done
  local s; s=$(kubectl -n "$NS" get statefulset scheduler -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  echo "${out}scheduler=${s:-0}"
}

# wait_for_job — polls until the Job reaches a terminal state. Short, retryable
# API calls rather than one long-lived stream: on a loaded machine the control
# plane can briefly refuse connections, and a dropped `logs -f` used to lose the
# whole point even though the Job itself had succeeded.
wait_for_job() {
  local deadline=$((SECONDS + ${BENCH_POINT_TIMEOUT:-1800}))
  while (( SECONDS < deadline )); do
    local succeeded failed
    succeeded=$(kubectl -n "$NS" get job hatch-bench -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
    failed=$(kubectl -n "$NS" get job hatch-bench -o jsonpath='{.status.failed}' 2>/dev/null || true)
    [[ "$succeeded" == "1" ]] && return 0
    [[ -n "$failed" && "$failed" != "0" ]] && return 1
    sleep 5
  done
  echo "  !! point timed out after ${BENCH_POINT_TIMEOUT:-1800}s" >&2
  return 1
}

# fetch_logs — retrieves the finished pod's logs, retrying transient API errors.
# The pod lingers (ttlSecondsAfterFinished), so this is safe to retry.
fetch_logs() {
  for _ in 1 2 3 4 5; do
    local out
    if out=$(kubectl -n "$NS" logs job/hatch-bench --tail=-1 2>/dev/null) && [[ -n "$out" ]]; then
      printf '%s' "$out"
      return 0
    fi
    sleep 5
  done
  return 1
}

# run_point <scenario> <count> <workers> <spread> <label> -> JSON on stdout
run_point() {
  local scenario="$1" count="$2" workers="$3" spread="$4" label="$5"
  local sched_replicas
  sched_replicas=$(kubectl -n "$NS" get statefulset scheduler -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 2)

  kubectl -n "$NS" delete job hatch-bench --ignore-not-found >/dev/null 2>&1
  # A finished Job's pod is deleted with it; wait so the next logs call cannot
  # pick up the previous point's output.
  for _ in $(seq 1 30); do
    kubectl -n "$NS" get pod -l app.kubernetes.io/component=bench --no-headers 2>/dev/null | grep -q . || break
    sleep 1
  done

  BENCH_IMAGE="$BENCH_IMAGE" \
  BENCH_SCENARIO="$scenario" BENCH_COUNT="$count" BENCH_WORKERS="$workers" \
  BENCH_SPREAD="$spread" BENCH_LABEL="$label" \
  BENCH_SCHEDULER_REPLICAS="$sched_replicas" \
  BENCH_SCHEDULE_LEAD="${BENCH_SCHEDULE_LEAD:-2m30s}" \
  BENCH_GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  BENCH_REPLICAS="$(replica_summary)" \
    envsubst < "$ROOT/scripts/bench-job.yaml" | kubectl apply -f - >/dev/null

  # Follow along for visibility only. Losing this stream no longer costs the
  # result — it is re-fetched from the finished pod below.
  ( kubectl -n "$NS" logs -f job/hatch-bench 2>/dev/null | sed 's/^/    /' >&2 ) || true

  if ! wait_for_job; then
    echo "  !! Job did not succeed for $scenario/$label" >&2
    fetch_logs | tail -20 >&2
    return 1
  fi

  local logs json
  logs=$(fetch_logs) || { echo "  !! could not read logs for $scenario/$label" >&2; return 1; }
  json=$(printf '%s\n' "$logs" | awk "/$JSON_BEGIN/{f=1;next} /$JSON_END/{f=0} f")
  if [[ -z "$json" ]]; then
    echo "  !! no result JSON from $scenario/$label — pod log follows" >&2
    printf '%s\n' "$logs" | tail -25 >&2
    return 1
  fi
  printf '%s' "$json"
}

# restore the stack to chart defaults, whatever a sweep left behind
restore_defaults() {
  log "Restoring chart defaults"
  kubectl -n "$NS" set env deployment/delivery-worker \
    DELIVERY_SEND_CONCURRENCY- MOCK_PROVIDER_LATENCY_MS- >/dev/null 2>&1
  scale delivery-worker 1
  note "delivery-worker back to 1 replica, env overrides cleared"
}

# ---------- modes ----------

cmd_one() {
  local scenario="${1:-e2e}" count="${2:-400}" workers="${3:-32}" spread="${4:-0s}" label="${5:-manual}"
  build_image
  mkdir -p "$RESULTS_DIR"
  mkdir -p "$RESULTS_DIR/results"
  local out="$RESULTS_DIR/results/$scenario-$(date -u +%Y%m%d-%H%M%S).json"
  run_point "$scenario" "$count" "$workers" "$spread" "$label" > "$out" || exit 1
  echo
  echo "result: $out"
}

cmd_all() {
  build_image
  mkdir -p "$RESULTS_DIR"
  local tmp; tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  local n=0

  # Each point is its own Job; the JSON accumulates and is aggregated at the end.
  # Points are ordered cheapest-first so a mistake surfaces early.
  emit() { n=$((n+1)); cp "$tmp/point.json" "$tmp/$(printf '%02d' "$n").json"; }
  point() { # <scenario> <count> <workers> <spread> <label>
    log "$1 — $5"
    if run_point "$1" "$2" "$3" "$4" "$5" > "$tmp/point.json"; then emit; return; fi
    note "retrying once"
    if run_point "$1" "$2" "$3" "$4" "$5" > "$tmp/point.json"; then emit; return; fi
    note "SKIPPED after two attempts: $1 / $5"
  }

  log "Hatch benchmark suite — roughly 50 minutes"
  note "each point needs a rollout, a ${BENCH_SCHEDULE_LEAD:-2m30s} schedule lead, and a scrape settle"

  # Counts are chosen per point so the measured window stays well clear of
  # startup noise without running for minutes: roughly count / expected rate
  # should land in the 10-40s range. A count sized for the slowest point would
  # make the fastest one a 2-second blur, and vice versa.

  # --- ingest: how fast schedules can be accepted ---
  scale delivery-worker 1
  for w in 8 32 128; do
    point ingest 5000 "$w" 0s "ingest, $w concurrent clients"
  done

  # --- delivery: capacity vs send concurrency, replicas fixed at 4 ---
  for spec in "1 800" "8 3000" "32 8000" "64 8000"; do
    set -- $spec
    set_env delivery-worker "DELIVERY_SEND_CONCURRENCY=$1"
    scale delivery-worker 4
    point delivery "$2" 64 0s "4 replicas, send concurrency $1"
  done

  # --- delivery: capacity vs replicas, concurrency fixed at 32 ---
  set_env delivery-worker "DELIVERY_SEND_CONCURRENCY=32"
  for spec in "1 3000" "2 5000" "8 8000"; do
    set -- $spec
    scale delivery-worker "$1"
    point delivery "$2" 64 0s "$1 replicas, send concurrency 32"
  done

  # --- e2e: punctuality when arrivals are spread, as real traffic would be ---
  scale delivery-worker 4
  point e2e 3000 32 60s "arrivals spread over 60s, 4 replicas, concurrency 32"

  restore_defaults

  log "Aggregating"
  go run ./cmd/benchreport "$tmp" "$RESULTS_DIR"
}

case "${1:-all}" in
  one) shift; cmd_one "$@" ;;
  all) cmd_all ;;
  *)   echo "usage: $0 {one <scenario> [count] [workers] [spread] [label] | all}" >&2; exit 2 ;;
esac
