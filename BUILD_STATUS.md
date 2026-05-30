# Build Status

Current phase: **Phase 3 — Delivery Workers (complete ✅)**
Next up: **Phase 4 — Retry Consumers**

| Phase | Title | Status |
|------:|---|---|
| 0 | Foundation (module, shared pkgs, helm charts, migrations, sqlc) | ✅ done |
| 1 | Scheduler API (router, auth, rate-limit, schedule + admin endpoints, instrumentation) | ✅ done |
| 2 | Scheduler Service (timer wheel, bbolt, Kafka produce, observability APIs) | ✅ done |
| 3 | Delivery Workers (batch consumer, provider router, mock + Resend providers, idempotency, retry-tier produce) | ✅ done |
| 4 | Retry Consumers (3 tier consumers + instrumentation) | ⏳ next |
| 5 | Reconciliation + Partition Archival crons | ⏸ pending |
| 6 | Grafana dashboards + Alertmanager wiring | ⏸ pending |

Verification is a single cumulative audit: `make verify` runs a host prelude
(build/vet/test/sqlc + pod status) then an in-cluster Job that checks
everything built so far over ClusterDNS. New phases append checks to
`internal/verify` rather than adding per-phase scripts.

Bring-up gates app pods on dependency readiness via init containers (no startup
CrashLoopBackOff), and DB migrations run in-cluster via the `db-migrate`
post-install/post-upgrade hook — no host-side port-forward required.
