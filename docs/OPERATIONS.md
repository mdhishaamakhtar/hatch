# Operations

How Hatch is built, deployed, and iterated on. See [README](../README.md) for
first-time setup and [ARCHITECTURE.md](ARCHITECTURE.md) for what each service does.

## Lifecycle

`observability` is infra — deploy it once and leave it. `hatch` is the app
stack (postgres/kafka/redis/api) you iterate on; `up` / `down` / `restart`
target it specifically so observability isn't torn down on every cycle.

| Command | Scope | What it does |
|---|---|---|
| `make up` | hatch | Three-phase `infra -> jobs -> pods` install/upgrade (assumes obs is up) |
| `make up-infra` | hatch | Bring up Postgres/Redis/Kafka only |
| `make up-jobs` | hatch | Run DB migrations and Kafka topic bootstrap |
| `make up-pods` | hatch | Bring up the service pods after jobs complete |
| `make down` | hatch | Uninstall `hatch` (PVCs kept, obs untouched) |
| `make restart` | hatch | `down` + `up`, keeps PVCs and obs |
| `make up-obs-crds` | obs | Refresh Prometheus Operator CRDs before observability install/upgrade |
| `make up-obs` | obs | Refresh CRDs, then install/upgrade `observability` (dashboards + alerts included) |
| `make down-obs` | obs | Uninstall `observability` (PVCs kept) |
| `make up-all` | both | First-time: obs then hatch |
| `make down-all` | both | Uninstall both releases (PVCs kept) |
| `make reset` | both | Nuclear: tear down both, wipe PVCs, redeploy clean |

`make up` runs in three phases: infra, jobs, then service pods. Migrations and
Kafka topic bootstrap run in the middle phase, after Postgres/Redis/Kafka are up
but before the application pods are created, which removes the race that was
causing the first reconciliation and partition archival runs to fail.

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
| `make build-scheduler` | Build the scheduler-service image |
| `make build-delivery-worker` | Build the delivery-worker image |
| `make build-retry-consumer` | Build the retry-consumer image |
| `make build-reconciliation-cron` | Build the reconciliation-cron image |
| `make build-partition-archival` | Build the partition-archival image |
| `make build-verify` | Build the in-cluster verify image |
| `make build` | Build every service image |
| `make run-api` | Run the scheduler-api locally against `HOST_*` DSNs (no k8s) |
| `make run-scheduler` | Run the scheduler-service locally as a single shard (`POD_INDEX=0 TOTAL_PODS=1`) |
| `make run-delivery-worker` | Run the delivery-worker locally against `HOST_*` DSNs (no k8s) |
| `make run-retry-consumer` | Run the retry-consumer locally against `HOST_*` brokers (no k8s) |
| `make run-reconciliation-cron` | Run the reconciliation-cron locally against `HOST_*` DSNs (no k8s) |
| `make run-partition-archival` | Run the partition-archival locally against `HOST_*` DSNs (no k8s) |
| `make gen-provider-key` | Print a fresh base64 Tink AES256-GCM keyset for `PROVIDER_CRED_KEY` |
| `make verify` | Run the full cumulative acceptance audit (see below) |

Each service image is built with a unique `hatch/<svc>:dev-<ts>` tag plus a
floating `:dev` alias.

## `make verify`

`make verify` is a single cumulative acceptance audit: a host prelude
(build/vet/test/sqlc + pod status) then an in-cluster Job covering migrations →
API golden path → scheduler → Kafka → delivery → retry → reconciliation →
partition archival → observability round-trips. New phases append checks to
`internal/verify` rather than adding per-phase scripts.

> The verify run performs a **live Resend send**, so it needs
> `VERIFY_RESEND_API_KEY` and a verified sender domain to pass.

## How the image flow works

`make build-api` produces a fresh `hatch/api:dev-<unix-ts>` image (and also
tags it as `hatch/api:dev` for convenience). The unique tag is written to
`.api-image-tag`; `make up` reads it and deploys that exact tag via
`helm --set api.image=...`. The same applies to the other `make build-*` targets
and to `make build-verify` (`hatch/verify:dev-<ts>`, `.verify-image-tag`), which
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
