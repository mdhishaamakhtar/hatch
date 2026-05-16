#!/usr/bin/env bash
# Apply the three Phase 0 probes (metric, log, trace) and wait for each to
# either complete (Job) or become Running (Pod). Idempotent — re-applies and
# replaces previous probe resources.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Remove any leftover probe resources from a prior run.
kubectl -n hatch delete --ignore-not-found pod metric-probe log-probe >/dev/null
kubectl -n hatch delete --ignore-not-found job trace-probe          >/dev/null
kubectl -n hatch delete --ignore-not-found configmap metric-probe-config >/dev/null

kubectl apply -f "$ROOT/scripts/probe/metric-probe.yaml" >/dev/null
kubectl apply -f "$ROOT/scripts/probe/log-probe.yaml"    >/dev/null
kubectl apply -f "$ROOT/scripts/probe/trace-probe.yaml"  >/dev/null

echo "Waiting for metric-probe to be Ready…"
kubectl -n hatch wait --for=condition=Ready pod/metric-probe --timeout=90s

echo "Waiting for log-probe to be Running…"
# Pod has restartPolicy: Never so "Ready" never fires once sleep exits.
# Wait for the container to at least start.
for _ in $(seq 1 30); do
  phase=$(kubectl -n hatch get pod log-probe -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "$phase" == "Running" || "$phase" == "Succeeded" ]] && break
  sleep 1
done

echo "Waiting for trace-probe Job to complete…"
kubectl -n hatch wait --for=condition=complete job/trace-probe --timeout=90s

echo "All probes applied."
