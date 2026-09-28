# Architecture

Hatch is a pipeline of single-purpose services. Postgres holds every schedule
and its state, Kafka hands schedules from one service to the next, Redis caches
clients and guards against duplicate sends, and each scheduler pod keeps its
timer wheel in bbolt. [OBSERVABILITY.md](OBSERVABILITY.md) covers how the
services are instrumented, and [OPERATIONS.md](OPERATIONS.md) how they are built
and deployed.

```
cmd/          one main package per binary: the six services, verify (the
              acceptance audit), bench and benchreport (benchmarks), and
              tinkgen (generates the credentials key)
internal/     one package per binary, and the packages they share:
  db            sqlc's code for queries/, and schedule ids
  service       what every binary's main does: logging, tracing, signals, HTTP
  httpx         health checks, admin auth, JSON errors
  kafka         the topics and the records on them
  provider      the email providers: mock and Resend
  crypto        encryption of provider credentials
  stack         a deployed stack as verify and bench drive it
migrations/   golang-migrate SQL
queries/      sqlc queries
helm/         hatch (the services and their infrastructure) and observability
scripts/      deployment helpers, and the verify and bench Jobs
benchmarks/   the committed benchmark results
```

## A schedule's path

1. A client `POST`s `/v1/schedules`. The API writes a `pending` row.
2. Every hour, each scheduler pod loads its share of the schedules due in the
   next hour into its wheel. Each second it publishes whatever has come due
   onto the `emails.due` topic.
3. A delivery worker reads the schedule id off `emails.due`, loads the row, and
   sends the email through one of the client's providers. The row ends
   `delivered`, `failed` or `cancelled`, or `retrying` on a retry tier topic.
4. The retry consumer puts a retrying schedule back onto `emails.due` once its
   tier's delay has passed.
5. The reconciliation cron puts back anything a crash stranded along the way,
   and the archival cron exports and drops each month's partition once every
   schedule in it has finished.

A Kafka record names one schedule: its key is the schedule's 16-byte id, and its
value the id as text. Everything else about the schedule is in Postgres.

## Schedule ids and partitions

`scheduled_emails` is partitioned by month on `deliver_at`, 1,200 months of it
created up front by migration 004. A query that filters on `id` alone would have
to look in every partition, so a schedule's id is a UUIDv7 whose timestamp is
its `deliver_at` rather than its creation time (`internal/db/schedule_id.go`).
Every lookup by id reads `deliver_at` back out of the id and filters on both, and
Postgres goes straight to one partition.

Idempotency keys can't be unique in the partitioned table, since a unique index
there must include `deliver_at`, so `schedule_idempotency` enforces them: a
create with a key claims it in the same statement that inserts the schedule.

## API

Clients authenticate with a bearer API key. The API stores only its SHA-256
digest: the key is 32 random bytes, so a slow hash like bcrypt would add latency
to every request and no security. Each client has a token bucket sized by its
`max_rps`.

Admin routes create clients and register their providers. A provider's
credentials are checked by building the provider, then encrypted with
`PROVIDER_CRED_KEY` (Tink AES-GCM, bound to the client and vendor), and the
client's cached state in Redis is evicted so the workers see the change.

## Scheduler

A StatefulSet of `TOTAL_PODS` pods. Pod `POD_INDEX` owns the schedules whose id
hashes to it, modulo the pod count.

The wheel is a bbolt file on the pod's volume, not a structure in memory: each
loaded schedule is a key made of the second it fires and its id, and bbolt keeps
keys sorted, so the schedules come due in key order. Two loops share it:

- **The poller** loads the pod's `pending` schedules due in the next hour, every
  hour, and records in the wheel how far ahead it has loaded. Each poll starts
  where the last one ended, so no schedule falls between two polls, and a pod
  that restarts picks up where it stopped. `POST /internal/poll` runs a poll now,
  to load schedules created since their hour was loaded.
- **The ticker** fires every second. It publishes everything due by now in one
  batch, then removes from the wheel what Kafka accepted. Taking everything due
  rather than just this second's schedules means a slow tick, or a schedule
  loaded after its second, still fires on the next tick. Removing only after the
  publish makes firing at-least-once; the delivery workers drop duplicates.

Admin routes, behind `ADMIN_API_KEY`: `POST /internal/poll`, and
`GET /internal/wheel/stats`, which reports the pod's shard and how many
schedules its wheel holds.

## Delivery worker

A Deployment whose pods share `emails.due` as the `delivery-workers` consumer
group. It reads up to 1,000 records at a time, loads their rows in one query,
and sends up to `DELIVERY_SEND_CONCURRENCY` of them at once. It commits the batch
only once every send in it has finished, so a crash replays it.

For each schedule it:

1. drops it if it has already finished, and otherwise marks it `processing`;
2. looks up the client's providers, from Redis, or from Postgres on a miss;
   an inactive client's schedule is `cancelled`;
3. claims the attempt in Redis (`idempotency:<id>:<attempt>`), so a replayed
   record cannot send the same attempt twice;
4. sends it and marks it `delivered`, or on a transient failure marks it
   `retrying` and parks it on the next retry tier, or once it has been through
   all three tiers, or the failure is permanent, marks it `failed`.

Every status change is guarded by the status it expects, so a change that loses
a race, say to a cancel, changes nothing.

The **router** picks the provider. Each (client, vendor) pair has a rate limiter
and a circuit breaker; the router prefers a vendor other than the one that just
failed, and the one with the most capacity to spare. When every provider is at
its limit or has its breaker open, the send waits in a retry tier instead of
failing. Two providers exist: `mock`, whose latency and error rates are set by
the `MOCK_PROVIDER_*` variables, and `resend`, which sends through Resend.

## Retry consumer

One consumer per tier (`emails.retry.1min`, `5min`, `30min`), each in its own
consumer group. Every `RETRY_INTERVALS` (1m, 5m and 30m by default; seconds in
development) a tier drains its topic back onto `emails.due`. It holds no retry
logic: the worker decides what happens next from the row's `retry_count`.

## Reconciliation cron

Every `RECON_INTERVAL` (24h), it finds schedules a crash stranded and puts them
back on `emails.due`:

- `pending` a while past their `deliver_at` (the scheduler never fired them), or
  `processing` for more than ten minutes (a worker died mid-send). No attempt
  finished, so their retry count is reset.
- `retrying` for more than two hours (their re-enqueue never happened). The
  failed attempt counted, so their retry count is kept.

Each moves the rows it finds to `processing`, so the next sweep leaves them be.
A schedule re-enqueued needlessly is harmless: the worker drops duplicates.

## Partition archival cron

Every `ARCHIVAL_INTERVAL` (30 days), it looks at each partition whose month is
over. If every schedule in it has finished, it exports the partition to
`ARCHIVE_DIR/<partition>.csv.gz` and drops it; otherwise it leaves it for the
next sweep.
