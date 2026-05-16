#!/usr/bin/env bash
# Builds the hatch-secrets k8s Secret from .env and applies it into both
# `hatch` and `observability` namespaces. Strips every HOST_* key first —
# those are localhost values for host-side dev tools, NOT for in-cluster
# services. Cluster services must talk via ClusterDNS only.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ROOT}/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing $ENV_FILE (copy .env.example to .env first)" >&2
  exit 1
fi

# Filter: drop blank/comment lines and any key starting with HOST_.
CLUSTER_ENV="$(mktemp)"
trap 'rm -f "$CLUSTER_ENV"' EXIT
grep -Ev '^\s*(#|HOST_|$)' "$ENV_FILE" > "$CLUSTER_ENV"

apply() {
  local ns="$1"
  kubectl get namespace "$ns" >/dev/null 2>&1 || kubectl create namespace "$ns"
  kubectl -n "$ns" create secret generic hatch-secrets \
    --from-env-file="$CLUSTER_ENV" \
    --dry-run=client -o yaml | kubectl apply -f -
}

apply hatch
apply observability
