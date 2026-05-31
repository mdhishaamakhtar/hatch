# Hatch

General-purpose, high-scale future email scheduler. Schedule emails from 1 hour
to years in advance, with at-least-once delivery and pluggable email providers.

## Tech Stack

[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestration-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Helm](https://img.shields.io/badge/Helm-4-0F1689?style=for-the-badge&logo=helm&logoColor=white)](https://helm.sh/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.9_KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17_Partitioned-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![bbolt](https://img.shields.io/badge/bbolt-1.4-4E5D6C?style=for-the-badge)](https://github.com/etcd-io/bbolt)
[![franz-go](https://img.shields.io/badge/franz--go-1.21-3A3A3A?style=for-the-badge)](https://github.com/twmb/franz-go)
[![Resend](https://img.shields.io/badge/Resend-Provider-000000?style=for-the-badge&logo=resend&logoColor=white)](https://resend.com/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Tracing-425CC7?style=for-the-badge&logo=opentelemetry&logoColor=white)](https://opentelemetry.io/)
[![Zap](https://img.shields.io/badge/Uber_Zap-1.28-232F3E?style=for-the-badge&logo=uber&logoColor=white)](https://github.com/uber-go/zap)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Loki](https://img.shields.io/badge/Loki-Logs-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/oss/loki/)
[![Tempo](https://img.shields.io/badge/Tempo-Traces-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/oss/tempo/)

A timer-wheel scheduler shards the keyspace across replicas and fires due emails
onto Kafka; stateless delivery workers send them through per-client providers
(`mock` + `resend`), with tiered retries, a reconciliation sweep for stranded
rows, and monthly partition archival. Design docs live on
[Notion](https://ruby-spectacles-2bc.notion.site/Hatch-34123f950a298115a7cec9d05a4d99f4).

## Quick start

Prerequisites:

- Docker Desktop with Kubernetes enabled (Settings → Kubernetes → Enable)
- `go` ≥ 1.25
- `helm` ≥ 4 (`brew install helm`)
- `kubectl` (bundled with Docker Desktop)
- `golang-migrate` (`brew install golang-migrate`)
- `sqlc` (`brew install sqlc`)
- `libpq` for `psql` (`brew install libpq && brew link --force libpq`)
- `redis` for `redis-cli` (`brew install redis`)

```sh
cp .env.example .env       # tweak placeholders if you need to
make up-all                # deploy observability + hatch in three phases
```

`make up-all` brings up the `observability` stack (Prometheus/Loki/Tempo/Grafana,
with dashboards and alerts auto-provisioned) then the `hatch` app stack. Day-to-day
you iterate with `make up` / `make down` / `make restart`, which target `hatch`
only and leave observability running. See [docs/OPERATIONS.md](docs/OPERATIONS.md)
for the full command reference.

## Local URLs

Always reachable (LoadBalancer, no port-forward needed):

| Service | URL |
|---|---|
| Scheduler API | http://localhost:9021 |
| Swagger UI | http://localhost:9021/swagger/index.html |
| Grafana | http://localhost:3000 (admin / admin) |
| Kafka UI | http://localhost:8080 |

Reachable after `make port-forward` (host tools / ad-hoc debugging):

| Service | URL |
|---|---|
| Postgres | localhost:5432 (user `hatch`, db `hatch`) |
| Redis | localhost:6379 |
| Kafka broker | localhost:9092 |

Hatch service ports start at `9021` and walk forward (9022 = scheduler admin,
9023 = delivery-worker, 9024 = retry-consumer, 9025 = reconciliation-cron,
9026 = partition-archival), keeping the conventional 3000/8080/9090 range free
for tooling. The scheduler-service runs as a 2-replica StatefulSet behind a
headless service, so each pod has a stable per-pod DNS name
(`scheduler-0.scheduler.hatch.svc.cluster.local:9022`, …) — that is how
`make verify` reaches each shard's admin API without a port-forward.

## Documentation

| Doc | Contents |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Per-service design: scheduler, delivery worker, retry consumers, reconciliation + archival crons, repo layout |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Lifecycle + common commands, image flow, env split, `make verify` |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Metrics/logs/traces stack, the Grafana dashboards, the alert list, enabling alert email |
| [docs/API.md](docs/API.md) | Endpoints, `deliver_at` timestamp format, link to the Swagger UI |
