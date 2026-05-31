# Hatch

General-purpose, high-scale future email scheduler. Schedule emails from 1 hour
to years in advance, with at-least-once delivery and pluggable email providers.

**Stack:** Go, PostgreSQL (partitioned), Kafka (KRaft), bbolt, Redis, on
Kubernetes — with a full Prometheus / Loki / Tempo / Grafana observability stack.

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
