#!/usr/bin/env bash
# Puts .env into the hatch-secrets Secret, which every Hatch pod loads as its
# environment. HOST_* keys are left out: they are localhost addresses, for
# tools on this machine.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ ! -f "$ROOT/.env" ]]; then
  echo "missing $ROOT/.env (copy .env.example to .env first)" >&2
  exit 1
fi

env_file="$(mktemp)"
trap 'rm -f "$env_file"' EXIT
grep -Ev '^\s*(#|HOST_|$)' "$ROOT/.env" > "$env_file"

kubectl get namespace hatch >/dev/null 2>&1 || kubectl create namespace hatch
kubectl -n hatch create secret generic hatch-secrets --from-env-file="$env_file" --dry-run=client -o yaml |
  kubectl apply -f -
