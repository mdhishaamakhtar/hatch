#!/usr/bin/env bash
# Start kubectl port-forwards for local access. Idempotent — kills existing
# forwards before starting new ones so re-running is safe.
set -euo pipefail

pkill -f "kubectl port-forward" || true
sleep 1

mkdir -p /tmp/hatch-pf
kubectl -n hatch port-forward svc/postgres 5432:5432 >/tmp/hatch-pf/postgres.log 2>&1 &
kubectl -n hatch port-forward svc/redis    6379:6379 >/tmp/hatch-pf/redis.log    2>&1 &
kubectl -n hatch port-forward svc/kafka    9092:9092 >/tmp/hatch-pf/kafka.log    2>&1 &

# Grafana NodePort 30000 + Kafka UI NodePort 30080 are reachable via
# docker-desktop's kubernetes service. We also expose them explicitly so the
# acceptance checks can hit localhost:3000 and :8080.
kubectl -n observability port-forward svc/observability-grafana 3000:80 >/tmp/hatch-pf/grafana.log 2>&1 &
kubectl -n observability port-forward svc/observability-kafka-ui 8080:80 >/tmp/hatch-pf/kafka-ui.log 2>&1 &
kubectl -n observability port-forward svc/observability-kps-prometheus 9090:9090 >/tmp/hatch-pf/prometheus.log 2>&1 &
# Loki + Tempo query endpoints — host-only, used for probe round-trip checks.
kubectl -n observability port-forward svc/observability-loki-gateway 3100:80 >/tmp/hatch-pf/loki.log 2>&1 &
kubectl -n observability port-forward svc/observability-tempo 3200:3200 >/tmp/hatch-pf/tempo.log 2>&1 &

sleep 2
echo "Port-forwards started:"
echo "  Postgres   localhost:5432"
echo "  Redis      localhost:6379"
echo "  Kafka      localhost:9092"
echo "  Grafana    http://localhost:3000  (admin/admin)"
echo "  Kafka UI   http://localhost:8080"
echo "  Prometheus http://localhost:9090"
