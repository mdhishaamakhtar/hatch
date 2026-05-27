# Build Status

Current phase: **Phase 2 — Scheduler Service (complete ✅)**
Next up: **Phase 3 — Delivery Workers**

| Phase | Title | Status |
|------:|---|---|
| 0 | Foundation (module, shared pkgs, helm charts, migrations, sqlc) | ✅ done |
| 1 | Scheduler API (router, auth, rate-limit, schedule + admin endpoints, instrumentation) | ✅ done |
| 2 | Scheduler Service (timer wheel, bbolt, Kafka produce, observability APIs) | ✅ done |
| 3 | Delivery Workers (batch consumer, provider router, Resend impl, idempotency) | ⏳ next |
| 4 | Retry Consumers (3 tier consumers + instrumentation) | ⏸ pending |
| 5 | Reconciliation + Partition Archival crons | ⏸ pending |
| 6 | Grafana dashboards + Alertmanager wiring | ⏸ pending |
