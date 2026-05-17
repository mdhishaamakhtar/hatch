# Build Status

Current phase: **Phase 1 — Scheduler API (complete ✅)**
Next up: **Phase 2 — Scheduler Service**

| Phase | Title | Status |
|------:|---|---|
| 0 | Foundation (module, shared pkgs, helm charts, migrations, sqlc) | ✅ done |
| 1 | Scheduler API (router, auth, rate-limit, schedule + admin endpoints, instrumentation) | ✅ done |
| 2 | Scheduler Service (timer wheel, bbolt, Kafka produce, observability APIs) | ⏳ next |
| 3 | Delivery Workers (batch consumer, provider router, Resend impl, idempotency) | ⏸ pending |
| 4 | Retry Consumers (3 tier consumers + instrumentation) | ⏸ pending |
| 5 | Reconciliation + Partition Archival crons | ⏸ pending |
| 6 | Grafana dashboards + Alertmanager wiring | ⏸ pending |
