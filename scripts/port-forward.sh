#!/usr/bin/env bash
# Forwards Postgres, Redis and Kafka to localhost, for `make migrate`,
# `make run-*` and debugging. The API, Grafana and Kafka UI are LoadBalancer
# services, reachable on localhost without this.
set -euo pipefail

pkill -f "kubectl port-forward" || true
sleep 1

mkdir -p /tmp/hatch-pf
kubectl -n hatch port-forward svc/postgres 5432:5432 >/tmp/hatch-pf/postgres.log 2>&1 &
kubectl -n hatch port-forward svc/redis    6379:6379 >/tmp/hatch-pf/redis.log    2>&1 &
kubectl -n hatch port-forward svc/kafka    9092:9092 >/tmp/hatch-pf/kafka.log    2>&1 &
sleep 2

echo "Forwarded:  Postgres localhost:5432, Redis localhost:6379, Kafka localhost:9092"
echo "Always on:  API http://localhost:9021, Grafana http://localhost:3000 (admin/admin), Kafka UI http://localhost:8080"
