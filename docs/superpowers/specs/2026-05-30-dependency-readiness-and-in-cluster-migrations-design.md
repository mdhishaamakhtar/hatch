# Dependency-readiness gating + in-cluster migrations

**Date:** 2026-05-30
**Status:** Approved, ready for implementation plan

## Problem

Two issues with the `make up` flow:

1. **CrashLoopBackOff on every fresh bring-up.** `make up` applies the
   postgres/redis/kafka StatefulSets and the api/scheduler workloads in the same
   Helm release. The app processes connect to their dependencies *eagerly at
   boot*:
   - `db.NewPool` calls `pool.Ping(ctx)` (`pkg/db/db.go`) and returns an error if
     Postgres is not yet accepting connections.
   - `redis.NewClient` calls `rueidis.NewClient`, which dials immediately and
     errors if Redis is not up.

   Either error propagates out of `run()` to `lg.Fatal`, the process exits, and
   the pod restarts — looping until the dependency happens to become ready.
   There are no init containers gating startup, so the app races its own
   dependencies on every bring-up.

2. **Migrations require manual port-forwarding.** Schema migrations run only via
   `make migrate`, host-side, against `HOST_DATABASE_URL` over a port-forward.
   There is no automated, in-cluster path, so a fresh `make reset` leaves the
   operator to port-forward and run `migrate` by hand.

## Goals

- App pods wait for their real dependencies before the main container starts —
  no CrashLoopBackOff during normal bring-up.
- Migrations run automatically in-cluster on `make up`, with no port-forwarding.
- Keep the host-side `make migrate` as an escape hatch.

## Non-goals

- Changing the app's connection code (the eager ping/dial stays; init containers
  gate it externally, which is the idiomatic k8s approach).
- Gating app pods on *schema* existence (only *connectivity*). See "Ordering" —
  schema-gating would deadlock against `helm --wait` + post-hooks. The brief
  post-`reset` window is tolerated.

## Design

### Part 1 — Init containers (end the crashloop)

Add init containers that block the main container until each real dependency
accepts a TCP connection, using a busybox probe loop:

```sh
until nc -z -w2 <svc> <port>; do echo "waiting for <svc>"; sleep 2; done
```

- **api** Deployment (`helm/hatch/templates/api/deployment.yaml`) → waits for
  `postgres:<postgres.servicePort>` and `redis:<redis.servicePort>`. Both are
  pinged/dialed at boot.
- **scheduler** StatefulSet (`helm/hatch/templates/scheduler/statefulset.yaml`)
  → waits for `postgres:<postgres.servicePort>` and `kafka:<kafka.brokerPort>`.
  Postgres is the boot-crash source; kafka is the produce target, waited on to
  avoid noisy produce errors at boot.

Service DNS uses in-namespace short names (`postgres`, `redis`, `kafka` —
confirmed in the chart). Ports are read from existing `.Values.*` — nothing
hardcoded. New value: `initWait.image: busybox:1.36`.

### Part 2 — Separate `db-migrate` Helm hook job

Two new templates under `helm/hatch/templates/migrations/`:

1. **`configmap.yaml`** — migration SQL packaged from disk via
   `{{ (.Files.Glob "migrations/*.sql").AsConfig | indent 2 }}`, mounted at
   `/migrations`. Regenerated on every `helm upgrade`, so it always matches the
   repo's `migrations/` directory.

2. **`job.yaml`** — a `post-install,post-upgrade` Helm hook:
   - `helm.sh/hook-weight: "0"` (runs before the kafka-topics job at weight 5).
   - `helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded`
     (re-created and re-run idempotently on each `make up`).
   - Image: `.Values.migrations.image` (`migrate/migrate:v4.18.3`).
   - `DATABASE_URL` injected from the `hatch-secrets` Secret via `secretKeyRef`
     (the ClusterDNS DSN, never the HOST_ DSN).
   - Command: a short `/bin/sh` retry loop around
     `migrate -path /migrations -database "$DATABASE_URL" up` (golang-migrate
     tracks applied versions, so `up` is a no-op when already current).
   - `backoffLimit`, `ttlSecondsAfterFinished`, `restartPolicy: OnFailure`
     mirror the existing kafka-topics-bootstrap job.

New `values.yaml` block:

```yaml
migrations:
  enabled: true
  image: migrate/migrate:v4.18.3
initWait:
  image: busybox:1.36
```

The job is gated on `.Values.migrations.enabled` (the configmap too), matching
the per-service `enabled` toggle convention.

### Ordering & why this is safe

Helm runs `post-install,post-upgrade` hooks *after* `--wait` confirms the normal
resources are Ready. On a fresh `make reset` (empty PVC, no schema):

- The api `/readyz` only pings Postgres/Redis (not tables), so it reaches Ready
  without schema.
- The scheduler poller tolerates a missing table (logs + retries) and its
  `/readyz` returns true, so it reaches Ready without schema.

So `--wait` succeeds, then the migrate hook applies the schema seconds later —
no deadlock, and the poller self-heals on its next cycle. On a normal `make up`
the schema already persists in the PVC, so `migrate up` is a no-op. Gating app
pods on schema existence is explicitly avoided because it would deadlock:
app-ready waits on schema → schema waits on the post-hook → the post-hook waits
on `helm --wait` (app-ready).

### Part 3 — Docs

- README: migrations now run automatically on `make up`; port-forward is no
  longer needed for migrations. `make migrate` remains a manual escape hatch.
- BUILD_STATUS: brief note. Kept basic per the per-phase docs convention.

## Acceptance

- `make reset` (wipes PVCs) brings the stack up with **zero** app-pod restarts
  attributable to dependency connectivity, and the schema is present afterward
  with no manual port-forward / `make migrate`.
- `make up` on an existing cluster re-runs the migrate hook as a clean no-op.
- `helm template ./helm/hatch` renders valid manifests for the new init
  containers, configmap, and job.

## Files touched

- `helm/hatch/templates/api/deployment.yaml` (add initContainers)
- `helm/hatch/templates/scheduler/statefulset.yaml` (add initContainers)
- `helm/hatch/templates/migrations/configmap.yaml` (new)
- `helm/hatch/templates/migrations/job.yaml` (new)
- `helm/hatch/values.yaml` (add `migrations`, `initWait`)
- `README.md`, `BUILD_STATUS.md` (docs upkeep)
