#!/usr/bin/env bash
# Start kubectl port-forwards for local access. Idempotent — kills existing
# forwards before starting new ones so re-running is safe.
#
# Note: the scheduler-API (localhost:9021) and Grafana (localhost:3000) are
# exposed via Service type=LoadBalancer and are reachable without this script.
set -euo pipefail

pkill -f "kubectl port-forward" || true
sleep 1

mkdir -p /tmp/hatch-pf
kubectl -n hatch port-forward svc/postgres 5432:5432 >/tmp/hatch-pf/postgres.log 2>&1 &
kubectl -n hatch port-forward svc/redis    6379:6379 >/tmp/hatch-pf/redis.log    2>&1 &
kubectl -n hatch port-forward svc/kafka    9092:9092 >/tmp/hatch-pf/kafka.log    2>&1 &

# Scheduler admin: one local port per pod (headless service has no
# cluster-IP). Ports walk forward from 9022 — scheduler-0 → 9022, scheduler-1
# → 9023. If the StatefulSet hasn't rolled out yet, the per-pod forwards fail
# silently into their log files, which is fine.
kubectl -n hatch port-forward pod/scheduler-0 9022:9022 >/tmp/hatch-pf/scheduler-0.log 2>&1 &
kubectl -n hatch port-forward pod/scheduler-1 9023:9022 >/tmp/hatch-pf/scheduler-1.log 2>&1 &

# Kafka UI keeps its NodePort (30080) + a port-forward so acceptance checks
# can hit localhost:8080 consistently.
kubectl -n observability port-forward svc/observability-kafka-ui 8080:80 >/tmp/hatch-pf/kafka-ui.log 2>&1 &
kubectl -n observability port-forward svc/observability-kps-prometheus 9090:9090 >/tmp/hatch-pf/prometheus.log 2>&1 &
# Loki + Tempo query endpoints — host-only, used for probe round-trip checks.
kubectl -n observability port-forward svc/observability-loki-gateway 3100:80 >/tmp/hatch-pf/loki.log 2>&1 &
kubectl -n observability port-forward svc/observability-tempo 3200:3200 >/tmp/hatch-pf/tempo.log 2>&1 &

sleep 2
echo "Always-on (LoadBalancer):"
echo "  API        http://localhost:9021"
echo "  Grafana    http://localhost:3000  (admin/admin)"
echo
echo "Port-forwards started:"
echo "  Postgres     localhost:5432"
echo "  Redis        localhost:6379"
echo "  Kafka        localhost:9092"
echo "  Scheduler-0  http://localhost:9022"
echo "  Scheduler-1  http://localhost:9023"
echo "  Kafka UI     http://localhost:8080"
echo "  Prometheus   http://localhost:9090"
