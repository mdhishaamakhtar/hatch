# Architecture

Hatch is a pipeline of single-purpose services connected by Postgres (durable
state), Kafka (work hand-off), Redis (client cache + idempotency), and bbolt
(per-pod timer-wheel persistence). This document covers each service; see
[OBSERVABILITY.md](OBSERVABILITY.md) for how they are instrumented and
[OPERATIONS.md](OPERATIONS.md) for how they are built and deployed.

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
already in the past — reconciliation owns recovery for past-due rows.

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
