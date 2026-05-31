#!/usr/bin/env bash
# Install/refresh kube-prometheus-stack CRDs out of band so the observability
# Helm release does not store them in the release Secret.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHART_DIR="${ROOT}/helm/observability/charts"
CHART_TGZ="$(find "$CHART_DIR" -maxdepth 1 -name 'kube-prometheus-stack-*.tgz' | sort | tail -n 1)"

if [[ -z "${CHART_TGZ}" || ! -f "${CHART_TGZ}" ]]; then
  echo "missing kube-prometheus-stack chart package under ${CHART_DIR}; run 'make deps' first" >&2
  exit 1
fi

crd_files=(
  "kube-prometheus-stack/charts/crds/crds/crd-alertmanagerconfigs.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-alertmanagers.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-podmonitors.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-probes.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-prometheusagents.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-prometheuses.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-prometheusrules.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-scrapeconfigs.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-servicemonitors.yaml"
  "kube-prometheus-stack/charts/crds/crds/crd-thanosrulers.yaml"
)

for file in "${crd_files[@]}"; do
  echo "→ refreshing ${file##*/}"
  tar -xOf "$CHART_TGZ" "$file" | kubectl apply --server-side --field-manager=hatch-observability-crds -f -
done

echo "→ observability CRDs refreshed"
