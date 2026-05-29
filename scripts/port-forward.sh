#!/usr/bin/env bash
# Start kubectl port-forwards for host-side access to the data services.
# Idempotent — kills existing forwards before starting new ones.
#
# Only the data services are forwarded here, for host tools and ad-hoc
# debugging (Postgres is what `make migrate` needs). Verification no longer
# relies on any port-forward — it runs in-cluster over ClusterDNS. The API,
# Grafana, and Kafka UI are exposed as LoadBalancer services and are reachable
# on localhost without this script.
set -euo pipefail

pkill -f "kubectl port-forward" || true
sleep 1

mkdir -p /tmp/hatch-pf
kubectl -n hatch port-forward svc/postgres 5432:5432 >/tmp/hatch-pf/postgres.log 2>&1 &
kubectl -n hatch port-forward svc/redis    6379:6379 >/tmp/hatch-pf/redis.log    2>&1 &
kubectl -n hatch port-forward svc/kafka    9092:9092 >/tmp/hatch-pf/kafka.log    2>&1 &

sleep 2
echo "Always-on (LoadBalancer — no port-forward needed):"
echo "  API        http://localhost:9021"
echo "  Grafana    http://localhost:3000  (admin/admin)"
echo "  Kafka UI   http://localhost:8080"
echo
echo "Port-forwards started (host tools / ad-hoc debugging):"
echo "  Postgres     localhost:5432   (used by 'make migrate')"
echo "  Redis        localhost:6379"
echo "  Kafka        localhost:9092"
