#!/usr/bin/env bash
# Puts migrations/ into the hatch-migrations ConfigMap, which the chart's
# migration Job applies. Helm cannot read files outside the chart itself.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
kubectl get namespace hatch >/dev/null 2>&1 || kubectl create namespace hatch
kubectl -n hatch create configmap hatch-migrations --from-file="$ROOT/migrations" --dry-run=client -o yaml |
  kubectl apply -f -
