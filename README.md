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
make up-all                # deploy observability + hatch; migrations run in-cluster
```

Migrations run automatically in-cluster via the `db-migrate` hook on every
`make up` — no port-forward needed. App pods wait for Postgres/Redis/Kafka to be
reachable before starting, so there's no startup CrashLoopBackOff. The scheduler
API and Grafana are exposed via `Service type=LoadBalancer` and are reachable on
`localhost:9021` and `localhost:3000` without `port-forward`.

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
| `make port-forward` | Forward Postgres / Redis / Kafka for host tools and ad-hoc debugging |
| `make status` | Pod status across both namespaces |
| `make logs SVC=postgres` | Tail logs for one component |
| `make migrate` | Apply pending DB migrations from the host (escape hatch; `make up` already applies them in-cluster) |
| `make migrate-down` | Roll back all migrations |
| `make sqlc` | Regenerate `gen/` from `queries/` + `migrations/` |
| `make swag-gen` | Regenerate OpenAPI spec under `docs/` from handler annotations |
| `make test` | `go test -race ./pkg/... ./internal/...` |
| `make build-api` | Build the scheduler-api image (unique `hatch/api:dev-<ts>` tag + `:dev` alias) |
| `make build-scheduler` | Build the scheduler-service image (unique `hatch/scheduler:dev-<ts>` tag + `:dev` alias) |
| `make build-delivery-worker` | Build the delivery-worker image (unique `hatch/delivery-worker:dev-<ts>` tag + `:dev` alias) |
| `make build-retry-consumer` | Build the retry-consumer image (unique `hatch/retry-consumer:dev-<ts>` tag + `:dev` alias) |
| `make build-reconciliation-cron` | Build the reconciliation-cron image (unique `hatch/reconciliation-cron:dev-<ts>` tag + `:dev` alias) |
| `make build-partition-archival` | Build the partition-archival image (unique `hatch/partition-archival:dev-<ts>` tag + `:dev` alias) |
| `make build-verify` | Build the in-cluster verify image (unique `hatch/verify:dev-<ts>` tag + `:dev` alias) |
| `make build` | Build every service image |
| `make run-api` | Run the scheduler-api locally against `HOST_*` DSNs (no k8s) |
| `make run-scheduler` | Run the scheduler-service locally as a single shard (`POD_INDEX=0 TOTAL_PODS=1`) |
| `make run-delivery-worker` | Run the delivery-worker locally against `HOST_*` DSNs (no k8s) |
| `make run-retry-consumer` | Run the retry-consumer locally against `HOST_*` brokers (no k8s) |
| `make run-reconciliation-cron` | Run the reconciliation-cron locally against `HOST_*` DSNs (no k8s) |
| `make run-partition-archival` | Run the partition-archival locally against `HOST_*` DSNs (no k8s) |
| `make gen-provider-key` | Print a fresh base64 Tink AES256-GCM keyset for `PROVIDER_CRED_KEY` |
| `make verify` | Run the full cumulative acceptance audit: a host prelude (build/vet/test/sqlc + pod status) then an in-cluster Job covering migrations → API golden path → scheduler → Kafka → delivery → retry → reconciliation → partition archival → observability round-trips |

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

The scheduler-service runs as a 2-replica StatefulSet behind a *headless*
service. Inside the cluster each pod has a stable per-pod DNS name
(`scheduler-0.scheduler.hatch.svc.cluster.local:9022`,
`scheduler-1…`), which is how `make verify` reaches each shard's
`/internal/wheel/stats` — no port-forward. For ad-hoc host access to one pod's
admin API, forward it directly:

```sh
kubectl -n hatch port-forward pod/scheduler-0 9022:9022
curl -H "Authorization: Bearer $ADMIN_API_KEY" http://localhost:9022/internal/wheel/stats
```

Hatch service ports start at `9021` and walk forward (9022 = scheduler admin
port, 9023 = delivery-worker admin port, 9024 = retry-consumer admin port, 9025 =
reconciliation-cron admin port, 9026 = partition-archival admin port). This keeps
the conventional 3000/8080/9090 range free for tooling — no host-side remapping
is ever needed.

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
(`.scheduler-image-tag`, `helm --set scheduler.image=...`) and to
`make build-verify` (`hatch/verify:dev-<ts>`, `.verify-image-tag`), which
`make verify` builds per-run and substitutes into the verify Job. Pods run with
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

## Delivery worker

Stateless `Deployment` that consumes `emails.due`, hydrates each schedule from
Postgres, sends it through a provider, and drives the `scheduled_emails` status
machine to a terminal state. Three goroutines:

1. **G1 — batch consumer**: polls `emails.due` (consumer group `delivery-workers`),
   accumulates up to `DELIVERY_BATCH_SIZE` records, hands the batch to G2, and
   commits offsets only after G2 acks (at-least-once).
2. **G2 — batch processor**: per row — `mark processing` → read-through client
   cache (Redis `client:{id}`, 5-min TTL) → Redis `SET NX` idempotency lock →
   provider-router select → send → `mark delivered`. On transient/rate-limited
   failure it marks `retrying` and re-enqueues to `emails.retry.{1min,5min,30min}`
   by attempt; after `DELIVERY_MAX_RETRIES` (3) attempts, or a permanent error, or
   no available provider, it marks `failed`. An inactive client marks `cancelled`.
3. **G3 — router ticker**: refills each provider's leaky bucket every
   `DELIVERY_PROVIDER_TICK`.

The **provider router** keeps a circuit breaker (`sony/gobreaker`) and a leaky
bucket per `(client, vendor)`. Selection filters to active vendors that have a
registered implementation, excludes any OPEN breaker, prefers a vendor other than
the last-failed one, and picks the one with the most tokens. The last-failed
exclusion is **best-effort**: it only kicks in when an alternative exists — if the
just-failed vendor is the client's *only* eligible provider, the exclusion is
dropped and the send is retried on it (a single-provider client must not be
stranded with `no_active_providers` after one transient blip; the retry tiers
exist precisely to reattempt transient failures). A genuinely unhealthy sole
provider still trips its breaker and yields no candidate. Two providers are
implemented: `mock` (offline, env-tuned latency/error rates) and `resend` (real
sends via the Resend API). Provider credentials are **per-client** — register them with
`POST /admin/clients/:id/providers` (`{"vendor":"resend","credentials":{"api_key":"re_…"}}`);
the API Tink-encrypts them and the worker decrypts with `PROVIDER_CRED_KEY` at
send time. Resend `from` addresses must be on a domain verified in Resend.

Admin surface on `:9023` — `/healthz`, `/readyz` (Postgres + Redis), `/metrics`.

## Retry consumers

Stateless `Deployment` that drains the three retry-tier topics and re-enqueues
each `schedule_id` back onto `emails.due`. One drain goroutine per tier, each
with its own durable consumer group (`retry-consumer-{1min,5min,30min}`) and a
drain ticker: on every tick it drains the tier topic and re-produces each record
to `emails.due` (carrying the original OTel trace context), committing offsets
only after a clean re-enqueue (at-least-once; duplicates are deduped by the
worker's Redis idempotency key). There is **no retry logic here** — exhaustion is
decided by the delivery worker on re-attempt from the Postgres `retry_count`, so
the consumer never touches Postgres or Redis.

Drain intervals are env-configurable (`RETRY_INTERVAL_{1MIN,5MIN,30MIN}`). They
default to the production `1m/5m/30m` in code; the dev cluster's Helm chart
overrides them to a few seconds so demos and `make verify` don't wait minutes for
a retry to flow through. A message's effective delay is bounded by its tier
interval — coarse by design.

Admin surface on `:9024` — `/healthz`, `/readyz` (Kafka ping), `/metrics`
(`hatch_retry_drained_total`, `hatch_retry_reenqueue_failures_total`,
`hatch_retry_drain_duration_seconds`, all by `tier`).

## Reconciliation cron

Stateless `Deployment` that runs a periodic sweep recovering schedule rows
stranded by a crash and re-enqueuing each onto `emails.due`. Two SQL passes:

- **Pass 1 (fresh attempt)** — rows stuck `pending` with an elapsed `deliver_at`,
  or `processing` with `updated_at` older than 10 minutes. No real attempt was
  made, so the pass resets `retry_count`/`last_provider` before re-enqueuing.
- **Pass 2 (orphaned retry)** — rows stuck `retrying` with `updated_at` older than
  2 hours (a retry consumer crashed before re-enqueuing). The pass preserves
  `retry_count`/`last_provider` — no extra retry budget.

Idempotent by design: every re-enqueue is deduped downstream by the delivery
worker's Redis `SET NX`, so a re-run never double-sends. The sweep interval is
`RECON_INTERVAL` (24h in production; the dev cluster sets it long and relies on
the run-on-boot sweep, since the acceptance verifier drives recovery in-process).
Admin surface on `:9025` — `/healthz`, `/readyz` (Postgres + Kafka ping),
`/metrics` (`hatch_recon_rows_recovered_total{pass}`,
`hatch_recon_run_duration_seconds`, `hatch_recon_last_run_timestamp`).

## Partition archival cron

Stateless `Deployment` that reclaims disk from old `scheduled_emails` partitions.
Each sweep walks the attached partitions (named `scheduled_emails_yYYYYmMM`) and,
for every one whose month is **fully in the past** *and* whose rows are **all
terminal** (`delivered`/`failed`/`cancelled`), archives it: `DETACH PARTITION` →
export to `<ARCHIVE_DIR>/<name>.csv.gz` via `COPY … TO STDOUT` → `DROP TABLE`. A
partition with any non-terminal row is left attached and retried next cycle.

The 1200 monthly partitions are pre-created with a 100-year forward runway
(migration 004); archival only ever drops fully-past partitions, so the
current/future runway is never touched. The interval is `ARCHIVAL_INTERVAL`
(monthly in production; long in the dev cluster, where the verifier exercises
archival in-process over isolated past partitions). Exports land on an `emptyDir`
at `/archive` in dev (a PVC or S3/GCS sync target in production). Admin surface on
`:9026` — `/healthz`, `/readyz` (Postgres ping), `/metrics`
(`hatch_db_active_partitions`, `hatch_archival_partitions_archived_total`,
`hatch_archival_run_duration_seconds`, `hatch_archival_last_run_timestamp`).

## Layout

```
cmd/         service entrypoints (api, scheduler, delivery-worker, verify, …)
internal/    service-specific business logic (incl. verify = in-cluster acceptance auditor)
pkg/         shared packages (logger, tracer, metrics, config, db, redis, kafka, wheelstore, provider, crypto)
migrations/  golang-migrate SQL files
queries/     sqlc query files
gen/         generated Go from sqlc
helm/        helm charts (hatch = data infra + services, observability = monitoring stack)
scripts/     port-forward, inject-secrets, verify (+ verify-job.yaml manifest)
```
