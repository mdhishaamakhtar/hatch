# Build Status

Current phase: **Phase 0 — Foundation (complete ✅)**
Next up: **Phase 1 — Scheduler API**

| Phase | Title | Status |
|------:|---|---|
| 0 | Foundation (module, shared pkgs, helm charts, migrations, sqlc) | ✅ done |
| 1 | Scheduler API (router, auth, rate-limit, schedule + admin endpoints, instrumentation) | ⏳ next |
| 2 | Scheduler Service (timer wheel, bbolt, Kafka produce, observability APIs) | ⏸ pending |
| 3 | Delivery Workers (batch consumer, provider router, Resend impl, idempotency) | ⏸ pending |
| 4 | Retry Consumers (3 tier consumers + instrumentation) | ⏸ pending |
| 5 | Reconciliation + Partition Archival crons | ⏸ pending |
| 6 | Grafana dashboards + Alertmanager wiring | ⏸ pending |
