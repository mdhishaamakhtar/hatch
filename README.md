# Hatch

General-purpose, high-scale future email scheduler. Schedule emails from 1 hour
to years in advance.

Stack: Go, PostgreSQL, Kafka (KRaft), bbolt, Redis, pluggable providers.

Status: see [BUILD_STATUS.md](BUILD_STATUS.md). Design docs live on [Notion](https://ruby-spectacles-2bc.notion.site/Hatch-34123f950a298115a7cec9d05a4d99f4)

---

## Prerequisites

- Docker Desktop with Kubernetes enabled (Settings → Kubernetes → Enable)
- `go` ≥ 1.25
- `helm` ≥ 4 (`brew install helm`)
- `kubectl` (bundled with Docker Desktop)
- `golang-migrate` (`brew install golang-migrate`)
- `sqlc` (`brew install sqlc`)
- `libpq` for `psql` (`brew install libpq && brew link --force libpq`)
- `redis` for `redis-cli` (`brew install redis`)

## First-time setup

```sh
cp .env.example .env       # tweak placeholders if you need to
make up-all                # deploy observability + hatch
make port-forward          # localhost ports for Postgres / Redis / Kafka / etc.
make migrate               # apply DB migrations
```

The scheduler API and Grafana are exposed via `Service type=LoadBalancer` and
are reachable on `localhost:9021` and `localhost:3000` without `port-forward`.

## Lifecycle

`observability` is infra — deploy it once and leave it. `hatch` is the app
stack (postgres/kafka/redis/api) you iterate on; `up` / `down` / `restart`
target it specifically so observability isn't torn down on every cycle.

| Command | Scope | What it does |
|---|---|---|
| `make up` | hatch | Inject secrets, install/upgrade `hatch` (assumes obs is up) |
| `make down` | hatch | Uninstall `hatch` (PVCs kept, obs untouched) |
| `make restart` | hatch | `down` + `up`, keeps PVCs and obs |
| `make up-obs` | obs | Install/upgrade `observability` |
| `make down-obs` | obs | Uninstall `observability` (PVCs kept) |
| `make up-all` | both | First-time: obs then hatch |
| `make down-all` | both | Uninstall both releases (PVCs kept) |
| `make reset` | both | Nuclear: tear down both, wipe PVCs, redeploy clean |

## Common commands

| Command | What it does |
|---|---|
| `make port-forward` | Forward Postgres / Redis / Kafka / Kafka UI / Prometheus / Loki / Tempo |
| `make status` | Pod status across both namespaces |
| `make logs SVC=postgres` | Tail logs for one component |
| `make migrate` | Apply pending DB migrations |
| `make migrate-down` | Roll back all migrations |
| `make sqlc` | Regenerate `gen/` from `queries/` + `migrations/` |
| `make swag-gen` | Regenerate OpenAPI spec under `docs/` from handler annotations |
| `make test` | `go test -race ./pkg/... ./internal/...` |
| `make build-api` | Build the scheduler-api image (unique `hatch/api:dev-<ts>` tag + `:dev` alias) |
| `make build-scheduler` | Build the scheduler-service image (unique `hatch/scheduler:dev-<ts>` tag + `:dev` alias) |
| `make build` | Build every service image |
| `make run-api` | Run the scheduler-api locally against `HOST_*` DSNs (no k8s) |
| `make run-scheduler` | Run the scheduler-service locally as a single shard (`POD_INDEX=0 TOTAL_PODS=1`) |
| `make gen-provider-key` | Print a fresh base64 Tink AES256-GCM keyset for `PROVIDER_CRED_KEY` |
| `make phase0-verify` | Run the full Phase 0 acceptance audit |
| `make phase1-verify` | Run the full Phase 1 acceptance audit (golden path + observability) |
| `make phase2-verify` | Run the full Phase 2 acceptance audit (API-driven schedule → wheel → Kafka, offset-safe consumer) |

## Local URLs

Always reachable (LoadBalancer, no port-forward needed):

| Service | URL |
|---|---|
| Scheduler API | http://localhost:9021 |
| Swagger UI | http://localhost:9021/swagger/index.html |
| Grafana | http://localhost:3000 (admin / admin) |

Reachable after `make port-forward`:

| Service | URL |
|---|---|
| Scheduler-0 admin | http://localhost:9022 |
| Scheduler-1 admin | http://localhost:9023 |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Loki gateway | http://localhost:3100 |
| Tempo HTTP | http://localhost:3200 |
| Postgres | localhost:5432 (user `hatch`, db `hatch`) |
| Redis | localhost:6379 |
| Kafka broker | localhost:9092 |

The scheduler-service runs as a 2-replica StatefulSet behind a *headless*
service, so each pod gets its own local port (one per pod, walking forward
from `9022`). Hit them directly:

```sh
curl -H "Authorization: Bearer $ADMIN_API_KEY" http://localhost:9022/internal/wheel/stats
curl -H "Authorization: Bearer $ADMIN_API_KEY" http://localhost:9023/internal/wheel/stats
```

Hatch service ports start at `9021` and walk forward (9022 = scheduler-0
admin, 9023 = scheduler-1 admin). This keeps the conventional 3000/8080/9090
range free for tooling — no host-side remapping is ever needed.

## API timestamp format

`deliver_at` on every schedule request and response is an int64 of
milliseconds since the Unix epoch (UTC). Validation:

- `0` or missing → `deliver_at_required`
- negative → `deliver_at_format`
- less than 1 hour in the future → `deliver_at_too_soon`

## How the image flow works

`make build-api` produces a fresh `hatch/api:dev-<unix-ts>` image (and also
tags it as `hatch/api:dev` for convenience). The unique tag is written to
`.api-image-tag`; `make up` reads it and deploys that exact tag via
`helm --set api.image=...`. The same applies to `make build-scheduler`
(`.scheduler-image-tag`, `helm --set scheduler.image=...`). Pods run with
`imagePullPolicy: Always`.

The unique tag matters because Docker Desktop's daemon image store and k8s'
containerd image store are separate — a floating `:dev` tag binding sticks to
whichever blob containerd cached first and rebuilds don't update k8s' view.
Pinning to a unique tag forces kubelet to resolve a new image binding on
every deploy.

## How the env split works

`.env` has two sections:

- `HOST_*` — `localhost` DSNs used by host tools (`make migrate`, `psql`, `redis-cli`).
- everything else — ClusterDNS values consumed by in-cluster services.

`scripts/inject-secrets.sh` strips every `HOST_*` key before populating the
`hatch-secrets` k8s Secret, so services in the cluster never see `localhost`
values.

## Scheduler service

Runs as a 2-replica StatefulSet. Each pod owns a deterministic hash slice of
the `scheduled_emails` keyspace via `POD_INDEX`/`TOTAL_PODS`. Three goroutines
per pod:

1. **G1 — poller**: every hour, queries Postgres for this pod's hash slice
   within the next-1h window.
2. **G2 — builder**: appends each (id, deliver_at) into the in-memory
   60×60 wheel and persists the slot to bbolt (per-pod PVC at `/var/lib/hatch`).
3. **G3 — ticker**: every second, drains the slot matching the current
   minute/second, produces a `{"schedule_id":"…"}` message to the
   `emails.due` Kafka topic (12 partitions), and signals G2 to clean the
   bbolt key.

On pod restart, G2 rebuilds the wheel from bbolt and drops any (mm, ss) slot
already in the past — Phase 5 reconciliation owns recovery for past-due rows.

Admin endpoints (Bearer `$ADMIN_API_KEY`):

| Endpoint | Purpose |
|---|---|
| `GET /internal/wheel/stats` | `pod_index`, `total_pods`, `occupied_slots`, `total_loaded` |
| `GET /internal/wheel/slots` | All occupied `(slot, count)` pairs |
| `GET /internal/wheel/slots/{mm}/{ss}` | UUID-stringified schedule_ids in a specific slot |

## Layout

```
cmd/         service entrypoints (api, scheduler, delivery-worker, …)
internal/    service-specific business logic
pkg/         shared packages (logger, tracer, metrics, config, db, redis, kafka, wheelstore, provider)
migrations/  golang-migrate SQL files
queries/     sqlc query files
gen/         generated Go from sqlc
helm/        helm charts (hatch = data infra + services, observability = monitoring stack)
scripts/     port-forward, inject-secrets, phaseN-verify, probes
```
