#!/usr/bin/env bash
# Creates/updates the hatch-migrations ConfigMap from repo-root migrations/*.sql
# in the hatch namespace. Helm's .Files cannot read files outside the chart, so
# the migration SQL is delivered out-of-band — same pattern as inject-secrets.sh.
# The db-migrate post-install/post-upgrade hook Job mounts this ConfigMap.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MIG_DIR="${ROOT}/migrations"
NS="${NS_HATCH:-hatch}"

if [[ ! -d "$MIG_DIR" ]]; then
  echo "missing $MIG_DIR" >&2
  exit 1
fi

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

kubectl -n "$NS" create configmap hatch-migrations \
  --from-file="$MIG_DIR" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "→ hatch-migrations ConfigMap synced from $MIG_DIR into $NS"
