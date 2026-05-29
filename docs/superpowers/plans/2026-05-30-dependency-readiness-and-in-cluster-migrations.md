# Dependency-readiness gating + in-cluster migrations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the dependency-race CrashLoopBackOff on `make up`, and apply DB migrations in-cluster automatically (no port-forward).

**Architecture:** Add busybox init containers to the api Deployment and scheduler StatefulSet that block until each real dependency accepts a TCP connection. Add a separate `db-migrate` Helm `post-install,post-upgrade` hook Job that runs `golang-migrate` against the ClusterDNS DSN, reading SQL from a `hatch-migrations` ConfigMap created out-of-band (mirroring `scripts/inject-secrets.sh`) because Helm's `.Files` cannot read repo-root `migrations/`.

**Tech Stack:** Helm 3, Kubernetes (StatefulSets/Deployments/Jobs/hooks), busybox, `migrate/migrate` (golang-migrate), bash, kubectl.

**Spec:** `docs/superpowers/specs/2026-05-30-dependency-readiness-and-in-cluster-migrations-design.md`

**Verification note:** Tasks 1–7 are verified with `helm template`/`helm lint`/`kubectl --dry-run=client`, which need **no running cluster**. Task 8 is the real-cluster acceptance run (`make reset`) and requires a local k8s cluster (kind/minikube/etc.) with the app images already built/loaded.

---

### Task 1: Add `migrations` and `initWait` values

**Files:**
- Modify: `helm/hatch/values.yaml` (append two top-level blocks after the `kafka:` block)

- [ ] **Step 1: Add the values blocks**

Append to the end of `helm/hatch/values.yaml`:

```yaml

# golang-migrate runs in-cluster as the db-migrate post-install/post-upgrade
# hook (helm/hatch/templates/migrations/job.yaml). The SQL is delivered via the
# hatch-migrations ConfigMap built out-of-band by scripts/sync-migrations.sh.
migrations:
  enabled: true
  image: migrate/migrate:v4.18.3

# Image used by the init containers that gate api/scheduler startup on
# dependency reachability (postgres/redis/kafka). busybox ships `nc`.
initWait:
  image: busybox:1.36
```

- [ ] **Step 2: Verify the chart still lints**

Run: `helm lint ./helm/hatch`
Expected: `1 chart(s) linted, 0 chart(s) failed`

- [ ] **Step 3: Commit**

```bash
git add helm/hatch/values.yaml
git commit -m "feat(helm): add migrations + initWait values"
```

---

### Task 2: Gate the api Deployment on postgres + redis

**Files:**
- Modify: `helm/hatch/templates/api/deployment.yaml` (insert `initContainers` between the pod `spec:` and `containers:`)

- [ ] **Step 1: Add the init container**

In `helm/hatch/templates/api/deployment.yaml`, replace:

```yaml
    spec:
      containers:
        - name: api
```

with:

```yaml
    spec:
      # Block startup until dependencies accept connections. The app pings
      # Postgres and dials Redis eagerly at boot (db.NewPool / rueidis.NewClient)
      # and Fatal-exits on failure, so without this it CrashLoopBackOffs while
      # racing its own dependencies on every bring-up.
      initContainers:
        - name: wait-for-deps
          image: {{ .Values.initWait.image | quote }}
          command: ["/bin/sh", "-c"]
          args:
            - |
              for dep in "postgres {{ .Values.postgres.servicePort }}" "redis {{ .Values.redis.servicePort }}"; do
                set -- $dep
                until nc -w 2 "$1" "$2" </dev/null 2>/dev/null; do
                  echo "waiting for $1:$2…"
                  sleep 2
                done
                echo "$1:$2 reachable"
              done
      containers:
        - name: api
```

- [ ] **Step 2: Render and verify the init container appears**

Run: `helm template hatch ./helm/hatch -s templates/api/deployment.yaml`
Expected: output contains an `initContainers:` block with `name: wait-for-deps`, the busybox image, and a loop referencing `postgres 5432` and `redis 6379`.

- [ ] **Step 3: Commit**

```bash
git add helm/hatch/templates/api/deployment.yaml
git commit -m "feat(helm): gate api startup on postgres+redis readiness"
```

---

### Task 3: Gate the scheduler StatefulSet on postgres + kafka

**Files:**
- Modify: `helm/hatch/templates/scheduler/statefulset.yaml` (insert `initContainers` between the pod `spec:` and `containers:`)

- [ ] **Step 1: Add the init container**

In `helm/hatch/templates/scheduler/statefulset.yaml`, replace:

```yaml
    spec:
      containers:
        - name: scheduler
```

with:

```yaml
    spec:
      # Block startup until dependencies accept connections. Postgres is the
      # boot-crash source (db.NewPool pings and Fatal-exits); kafka is the
      # produce target, waited on to avoid noisy produce errors at boot.
      initContainers:
        - name: wait-for-deps
          image: {{ .Values.initWait.image | quote }}
          command: ["/bin/sh", "-c"]
          args:
            - |
              for dep in "postgres {{ .Values.postgres.servicePort }}" "kafka {{ .Values.kafka.brokerPort }}"; do
                set -- $dep
                until nc -w 2 "$1" "$2" </dev/null 2>/dev/null; do
                  echo "waiting for $1:$2…"
                  sleep 2
                done
                echo "$1:$2 reachable"
              done
      containers:
        - name: scheduler
```

- [ ] **Step 2: Render and verify the init container appears**

Run: `helm template hatch ./helm/hatch -s templates/scheduler/statefulset.yaml`
Expected: output contains an `initContainers:` block with `name: wait-for-deps` and a loop referencing `postgres 5432` and `kafka 9092`.

- [ ] **Step 3: Commit**

```bash
git add helm/hatch/templates/scheduler/statefulset.yaml
git commit -m "feat(helm): gate scheduler startup on postgres+kafka readiness"
```

---

### Task 4: Add the `db-migrate` hook Job template

**Files:**
- Create: `helm/hatch/templates/migrations/job.yaml`

- [ ] **Step 1: Create the Job template**

Create `helm/hatch/templates/migrations/job.yaml`:

```yaml
{{- if .Values.migrations.enabled }}
# Applies golang-migrate migrations in-cluster as a post-install/post-upgrade
# hook — no host-side port-forward needed. SQL comes from the hatch-migrations
# ConfigMap created out-of-band by scripts/sync-migrations.sh (Helm .Files
# cannot read repo-root migrations/). Idempotent: golang-migrate tracks applied
# versions, so re-running on every `make up` is a no-op ("no change").
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migrate
  labels:
    app.kubernetes.io/part-of: hatch
    app.kubernetes.io/component: db-migrate
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "0"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 6
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: hatch
        app.kubernetes.io/component: db-migrate
    spec:
      restartPolicy: OnFailure
      volumes:
        - name: migrations
          configMap:
            name: hatch-migrations
      containers:
        - name: migrate
          image: {{ .Values.migrations.image | quote }}
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              echo "applying migrations from /migrations…"
              for i in $(seq 1 30); do
                if migrate -path /migrations -database "$DATABASE_URL" up; then
                  echo "migrations applied"
                  exit 0
                fi
                echo "  attempt $i failed; retrying in 2s…"
                sleep 2
              done
              echo "migrations failed after retries" >&2
              exit 1
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: hatch-secrets
                  key: DATABASE_URL
          volumeMounts:
            - name: migrations
              mountPath: /migrations
{{- end }}
```

- [ ] **Step 2: Render and verify the Job**

Run: `helm template hatch ./helm/hatch -s templates/migrations/job.yaml`
Expected: a `kind: Job` named `db-migrate` with the `helm.sh/hook: post-install,post-upgrade` annotation, image `migrate/migrate:v4.18.3`, a `DATABASE_URL` env from `secretKeyRef` (name `hatch-secrets`, key `DATABASE_URL`), and a `migrations` volume mounting ConfigMap `hatch-migrations` at `/migrations`.

- [ ] **Step 3: Verify it disappears when disabled**

Run: `helm template hatch ./helm/hatch --set migrations.enabled=false | grep -c "name: db-migrate" || true`
Expected: `0` — the gated template renders nothing when `migrations.enabled=false`, confirming the toggle works.

- [ ] **Step 4: Commit**

```bash
git add helm/hatch/templates/migrations/job.yaml
git commit -m "feat(helm): add in-cluster db-migrate post-install/upgrade hook"
```

---

### Task 5: Add `scripts/sync-migrations.sh`

**Files:**
- Create: `scripts/sync-migrations.sh`

- [ ] **Step 1: Create the script**

Create `scripts/sync-migrations.sh`:

```bash
#!/usr/bin/env bash
# Creates/updates the hatch-migrations ConfigMap from repo-root migrations/*.sql
# in the hatch namespace. Helm's .Files cannot read files outside the chart, so
# the migration SQL is delivered out-of-band — same pattern as inject-secrets.sh.
# The db-migrate post-install/post-upgrade hook Job mounts this ConfigMap.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MIG_DIR="${ROOT}/migrations"
NS="${NS_HATCH:-hatch}"

if [[ ! -d "$MIG_DIR" ]]; then
  echo "missing $MIG_DIR" >&2
  exit 1
fi

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

kubectl -n "$NS" create configmap hatch-migrations \
  --from-file="$MIG_DIR" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "→ hatch-migrations ConfigMap synced from $MIG_DIR into $NS"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x scripts/sync-migrations.sh`

- [ ] **Step 3: Verify syntax**

Run: `bash -n scripts/sync-migrations.sh`
Expected: no output, exit 0.

- [ ] **Step 4: Verify the ConfigMap content renders (no cluster needed)**

Run: `kubectl create configmap hatch-migrations --from-file=migrations/ --dry-run=client -o yaml`
Expected: a `kind: ConfigMap` whose `data:` keys include all migration files, e.g. `001_create_clients.up.sql`, `001_create_clients.down.sql`, … through `005_clients_api_key_lookup.up.sql` (10 keys total).

- [ ] **Step 5: Commit**

```bash
git add scripts/sync-migrations.sh
git commit -m "feat(scripts): sync-migrations.sh builds hatch-migrations ConfigMap"
```

---

### Task 6: Wire `sync-migrations.sh` into `make up`

**Files:**
- Modify: `Makefile` (the `up` target)

- [ ] **Step 1: Call the script before `helm upgrade`**

In `Makefile`, in the `up` target, replace:

```makefile
up: ## Deploy hatch (app stack: postgres/kafka/redis/api/scheduler). Assumes obs is already up.
	@./scripts/inject-secrets.sh
	@API_TAG=$$([ -f .api-image-tag ] && cat .api-image-tag || echo dev); \
```

with:

```makefile
up: ## Deploy hatch (app stack: postgres/kafka/redis/api/scheduler). Assumes obs is already up.
	@./scripts/inject-secrets.sh
	@./scripts/sync-migrations.sh
	@API_TAG=$$([ -f .api-image-tag ] && cat .api-image-tag || echo dev); \
```

- [ ] **Step 2: Verify the target invokes the script**

Run: `make -n up`
Expected: the dry-run output lists `./scripts/inject-secrets.sh` immediately followed by `./scripts/sync-migrations.sh`, then the `helm upgrade --install hatch …` line.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(make): sync migrations ConfigMap before helm upgrade"
```

---

### Task 7: Docs upkeep (README + BUILD_STATUS)

**Files:**
- Modify: `README.md` (First-time setup block; Common commands table)
- Modify: `BUILD_STATUS.md` (brief bring-up note)

- [ ] **Step 1: Simplify First-time setup**

In `README.md`, replace:

````markdown
```sh
cp .env.example .env       # tweak placeholders if you need to
make up-all                # deploy observability + hatch
make port-forward          # localhost ports for Postgres / Redis / Kafka / etc.
make migrate               # apply DB migrations
```

The scheduler API and Grafana are exposed via `Service type=LoadBalancer` and
are reachable on `localhost:9021` and `localhost:3000` without `port-forward`.
````

with:

````markdown
```sh
cp .env.example .env       # tweak placeholders if you need to
make up-all                # deploy observability + hatch; migrations run in-cluster
```

Migrations run automatically in-cluster via the `db-migrate` hook on every
`make up` — no port-forward needed. App pods wait for Postgres/Redis/Kafka to be
reachable before starting, so there's no startup CrashLoopBackOff. The scheduler
API and Grafana are exposed via `Service type=LoadBalancer` and are reachable on
`localhost:9021` and `localhost:3000` without `port-forward`.
````

- [ ] **Step 2: Update the Common commands table**

In `README.md`, replace:

```markdown
| `make port-forward` | Forward Postgres / Redis / Kafka for host tools (`make migrate` and ad-hoc debugging) |
```

with:

```markdown
| `make port-forward` | Forward Postgres / Redis / Kafka for host tools and ad-hoc debugging |
```

Then replace:

```markdown
| `make migrate` | Apply pending DB migrations |
```

with:

```markdown
| `make migrate` | Apply pending DB migrations from the host (escape hatch; `make up` already applies them in-cluster) |
```

- [ ] **Step 3: Add a bring-up note to BUILD_STATUS**

In `BUILD_STATUS.md`, after the paragraph that begins "Verification is a single cumulative audit", add:

```markdown

Bring-up gates app pods on dependency readiness via init containers (no startup
CrashLoopBackOff), and DB migrations run in-cluster via the `db-migrate`
post-install/post-upgrade hook — no host-side port-forward required.
```

- [ ] **Step 4: Verify the edits landed**

Run: `grep -n "in-cluster" README.md BUILD_STATUS.md`
Expected: matches in both files describing automatic in-cluster migrations.

- [ ] **Step 5: Commit**

```bash
git add README.md BUILD_STATUS.md
git commit -m "docs: in-cluster migrations + dependency-readiness bring-up"
```

---

### Task 8: Whole-chart validation + real-cluster acceptance

**Files:** none (verification only)

- [ ] **Step 1: Lint and render the whole chart**

Run: `helm lint ./helm/hatch && helm template hatch ./helm/hatch >/dev/null && echo OK`
Expected: lint passes and full-chart render succeeds with `OK` (no template errors).

- [ ] **Step 2 (requires a running cluster): Clean redeploy and watch for crashloops**

Run: `make reset` (tears down both releases, wipes PVCs, redeploys clean)
Expected: `make reset` completes; `helm upgrade … --wait` returns success. Then run `make status` and confirm api/scheduler pods are `Running` with **0 restarts** attributable to dependency connectivity (init containers held them until deps were ready).

- [ ] **Step 3 (requires a running cluster): Confirm migrations ran in-cluster, no port-forward**

Run: `kubectl -n hatch get jobs db-migrate -o jsonpath='{.status.succeeded}'; echo`
Expected: `1` (the hook Job completed). Optionally `kubectl -n hatch logs job/db-migrate` shows `migrations applied` (or `no change`).

- [ ] **Step 4 (requires a running cluster): Run the cumulative auditor**

Run: `make verify`
Expected: the cumulative acceptance audit passes — it exercises the schema (migrations → API golden path → scheduler → Kafka), proving migrations were applied without any manual `make migrate`/port-forward.

- [ ] **Step 5: Final commit (if any verification tweaks were needed)**

```bash
git add -A
git commit -m "chore: verify dependency-readiness + in-cluster migrations" || echo "nothing to commit"
```
