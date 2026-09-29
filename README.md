# Hatch

General-purpose, high-scale future email scheduler. Schedule emails from 1 hour
to years in advance, with at-least-once delivery and pluggable email providers.

## Tech Stack

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
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

A timer-wheel scheduler, sharded across replicas, fires each email onto Kafka
when it falls due, and delivery workers send it through the client's own
providers (`mock` or `resend`). Failed sends climb three retry tiers, a
reconciliation cron recovers schedules a crash stranded, and an archival cron
exports and drops each month's partition once it is done with. Design docs live
on [Notion](https://ruby-spectacles-2bc.notion.site/Hatch-34123f950a298115a7cec9d05a4d99f4).

## Quick start

Prerequisites:

- Docker Desktop with Kubernetes enabled (Settings → Kubernetes → Enable)
- `go` 1.27 or later
- `helm` (`brew install helm`)
- `kubectl` (bundled with Docker Desktop)
- `sqlc` (`brew install sqlc`), to regenerate `internal/db`
- `golang-migrate` (`brew install golang-migrate`), for `make migrate`

```sh
cp .env.example .env    # then set PROVIDER_CRED_KEY: make gen-provider-key
make build              # build the services' images
make up-all             # deploy the observability stack, then Hatch
make verify             # check it all works
```

`make up-all` deploys two Helm releases: `observability` (Prometheus, Loki, Tempo
and Grafana, with Hatch's dashboards and alerts) and `hatch`. From then on,
`make up` and `make down` redeploy `hatch` alone. [docs/OPERATIONS.md](docs/OPERATIONS.md)
has the rest.

## Local URLs

| Service | URL |
|---|---|
| API | http://localhost:9021 |
| Swagger UI | http://localhost:9021/swagger/index.html |
| Grafana | http://localhost:3000 (admin / admin) |
| Kafka UI | http://localhost:8080 |

After `make port-forward`, Postgres is on localhost:5432 (user and database
`hatch`), Redis on localhost:6379 and Kafka on localhost:9092.

The services listen on 9021 to 9026: the API, the scheduler, the delivery
worker, the retry consumer, the reconciliation cron and the archival cron. Each
serves `/healthz`, `/readyz` and `/metrics`.

## Benchmarks

```sh
make bench SCENARIO=delivery COUNT=8000   # one scenario
make bench-all                            # the reference suite (about an hour)
```

The benchmarks run as a Job in the cluster against the deployed stack.
`make bench-all` scales the delivery workers between runs and writes
[benchmarks/reference.md](benchmarks/reference.md), every number in which comes
from the harness. Watch a run at http://localhost:3000/d/hatch-benchmark.
[docs/BENCHMARKS.md](docs/BENCHMARKS.md) explains what the numbers mean, and
what bounds each stage.

## Documentation

| Doc | Contents |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How a schedule moves through the services, and how each works |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Building, deploying, configuring and checking the stack |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Metrics, logs, traces, the dashboards and the alerts |
| [docs/API.md](docs/API.md) | The API's routes and how to schedule an email |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | What the system sustains, what bounds each stage, and how to measure it |
