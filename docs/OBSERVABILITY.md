# Observability

Hatch ships a full metrics / logs / traces stack and provisions its Grafana
dashboards and Prometheus alerts automatically. Everything here is installed by
`make up-obs` (the `observability` Helm release) — there are no manual Grafana
clicks to reproduce it.

## Stack

| Component | Role |
|---|---|
| Prometheus (kube-prometheus-stack) | Scrapes `/metrics` on every service, stores 7d |
| Loki + Promtail | Structured JSON log aggregation (Promtail tails pod stdout) |
| Tempo | Distributed traces via OTLP gRPC, linked from logs by `trace_id` |
| Grafana | Dashboards, log/trace explore, alert view (LoadBalancer on `:3000`) |
| Alertmanager | Routes fired alerts to a receiver (email) |

Prometheus discovers targets by pod annotation (`prometheus.io/scrape: "true"`),
so each service is scraped on its admin port without a ServiceMonitor. All app
metrics live in the `hatch_*` namespace.

## Dashboards

Dashboard JSON lives in [`helm/observability/dashboards/`](../helm/observability/dashboards).
The [`grafana-dashboards.yaml`](../helm/observability/templates/grafana-dashboards.yaml)
template wraps each file into a ConfigMap labelled `grafana_dashboard: "1"`;
Grafana's sidecar watches for that label and imports them into a **Hatch** folder.
To add or edit a dashboard, drop/modify a JSON file and re-run `make up-obs`.

| Dashboard | What it shows |
|---|---|
| Global Performance Overview | e2e latency p50/p95/p99 + heatmap, delivered/failed/retrying rates, batch + wheel pipeline health, failure breakdown, circuit-breaker state |
| Provider Health | Per-provider send latency, success/outcome rates, circuit-breaker timeline, leaky-bucket tokens |
| Scheduler Service | Per-pod poll duration, emails loaded, wheel occupancy, Kafka produce latency/failures, pod-identity sanity table |
| Logs Explorer (Loki) | Saved log views: all ERROR, by `schedule_id`, by `client_id`, delivery WARN+ERROR, recon run history |

> The Notion design lists an Infrastructure dashboard (Postgres/Redis/Kafka).
> It is intentionally not built here: those panels need `postgres_exporter`,
> `redis_exporter`, and a Kafka exporter, which this deployment does not run.

## Alerts

Alerts are a single `PrometheusRule` CR,
[`prometheus-rules.yaml`](../helm/observability/templates/prometheus-rules.yaml),
auto-discovered by Prometheus (the chart's `ruleSelector` is open). They appear
under **Grafana → Alerting → Alert rules** and in the Prometheus/Alertmanager UIs.

Only alerts backed by metrics that actually exist are defined:

| Alert | Severity | Condition |
|---|---|---|
| HatchHighE2ELatencyCritical | critical | p99 e2e latency > 30s |
| HatchHighE2ELatencyWarning | warning | p95 e2e latency > 10s |
| HatchCircuitBreakerOpen | warning | any provider breaker == OPEN for 1m |
| HatchHighRetryRate | warning | retries > 10% of sends over 5m |
| HatchTerminalFailuresSpiking | warning | terminal `failed` rate > 0 sustained 5m |
| HatchNoActiveProviderFailures | critical | `failed{reason="no_active_providers"}` > 0 |
| HatchRedisUnavailable | critical | client-cache `result="unavailable"` > 0 |
| HatchKafkaProduceFailures | critical | scheduler produce failures > 0 sustained |
| HatchClientRateLimitingSustained | warning | a client 429-limited continuously 10m |
| HatchReconciliationStale | critical | `recon_last_run_timestamp` older than 25h |
| HatchArchivalStale | warning | `archival_last_run_timestamp` older than 35d |
| HatchPartitionCountOutOfRange | warning | active partitions < 2 or > 6 |

Dropped vs. the Notion list (no exporter / no metric): Kafka consumer-lag spike,
Postgres connection-pool, and the ERROR-log-rate catch-all.

## Enabling alert email

Alertmanager routing is wired in
[`values.yaml`](../helm/observability/values.yaml) under
`kps.alertmanager.config` with a `hatch-email` receiver, but the SMTP settings
are **placeholders** (`REPLACE_ME`) — no credentials are committed. Alerts fire
and are visible in the UIs regardless; only email delivery needs real values.

To enable email, supply real SMTP settings on the install — e.g. keep them in an
untracked overrides file and pass it through:

```sh
# obs-secrets.yaml (gitignored)
kps:
  alertmanager:
    config:
      global:
        smtp_smarthost: "smtp.yourhost.com:587"
        smtp_from: "hatch-alerts@yourdomain.com"
        smtp_auth_username: "apikey"
        smtp_auth_password: "••••••"
      receivers:
        - name: hatch-email
          email_configs:
            - to: "you@yourdomain.com"
              send_resolved: true
```

```sh
helm upgrade --install observability ./helm/observability \
  --namespace observability -f obs-secrets.yaml --reuse-values
```

With the placeholders left in place Alertmanager will log SMTP send failures when
an alert fires — harmless on a local cluster.

## Logs & traces

All services log JSON via zap with a stable shape (`service`, `level`, `ts`,
`msg`, plus contextual `schedule_id` / `client_id` / `provider` / `trace_id`).
Promtail ships stdout to Loki. The Loki datasource is configured with a derived
`TraceID` field, so an ERROR log links straight to its Tempo trace; the Tempo
datasource links back to logs by `service.name`. Use the **Logs Explorer**
dashboard or Grafana's Explore view to pivot across them.
