# Observability

`make up-obs` deploys the observability stack, with Hatch's dashboards and
alerts already loaded.

| Component | Role |
|---|---|
| Prometheus (kube-prometheus-stack) | Scrapes every service's `/metrics` and keeps 7 days |
| Loki and Promtail | Collect the pods' JSON logs |
| Tempo | Stores traces, sent over OTLP to `OTLP_ENDPOINT` |
| Grafana | Dashboards, and exploring logs and traces, at http://localhost:3000 |
| Alertmanager | Sends alerts that fire to a receiver, by email |

Prometheus scrapes any pod annotated `prometheus.io/scrape: "true"`, on the port
in `prometheus.io/port`, and labels each series with the `pod` it came from.
Hatch's metrics are all named `hatch_*`, and each is declared, with its help
text, in the package that records it.

There are no Postgres, Redis or Kafka dashboards or alerts: they would need
exporters this deployment doesn't run.

## Dashboards

Each JSON file in [`helm/observability/dashboards/`](../helm/observability/dashboards)
becomes a dashboard in Grafana's Hatch folder. Edit one and run `make up-obs`.

| Dashboard | What it shows |
|---|---|
| Global Performance Overview | Lateness (deliver_at to delivered) percentiles and heatmap, delivery outcomes, batch and wheel health, failures by reason, circuit breakers |
| Provider Health | Send latency and outcomes per provider, circuit breakers, sends deferred for lack of a provider |
| Scheduler Service | Per pod: polls, schedules fired and waiting, Kafka publish latency and failures, which shard it owns |
| Logs Explorer | Errors, the logs of one schedule or client, the delivery workers' warnings, the reconciliation cron's sweeps |
| Benchmark | Each stage of the pipeline side by side, for watching `make bench` |

## Alerts

The alerts are one `PrometheusRule`,
[`prometheus-rules.yaml`](../helm/observability/templates/prometheus-rules.yaml).
They show in Grafana under Alerting, and in the Prometheus and Alertmanager UIs.

| Alert | Severity | Fires when |
|---|---|---|
| HatchHighE2ELatencyCritical | critical | p99 lateness is over 30s |
| HatchHighE2ELatencyWarning | warning | p95 lateness is over 10s |
| HatchCircuitBreakerOpen | warning | a provider's circuit breaker has been open for 1m |
| HatchHighRetryRate | warning | retries are over 10% of sends |
| HatchTerminalFailuresSpiking | warning | schedules keep failing for good, for 5m |
| HatchNoActiveProviderFailures | critical | schedules fail because their client has no provider |
| HatchRedisUnavailable | critical | the delivery workers cannot reach Redis |
| HatchKafkaProduceFailures | critical | the scheduler cannot publish to `emails.due` |
| HatchClientRateLimitingSustained | warning | requests have been rate limited for 10m straight |
| HatchReconciliationStale | critical | the reconciliation cron has not swept for 25h |
| HatchArchivalStale | warning | the archival cron has not swept for 35 days |
| HatchPartitionRunwayShort | warning | fewer than ten years of partitions are left |

### Alert email

Alertmanager's `hatch-email` receiver is set up in
[`values.yaml`](../helm/observability/values.yaml), with placeholder SMTP
settings: alerts fire and show in the UIs, but no email goes out, and
Alertmanager logs the failed sends. To send them, pass real settings from a file
kept out of git:

```yaml
# obs-secrets.yaml
kps:
  alertmanager:
    config:
      global:
        smtp_smarthost: smtp.example.com:587
        smtp_from: hatch-alerts@example.com
        smtp_auth_username: apikey
        smtp_auth_password: "…"
      receivers:
        - name: hatch-email
          email_configs:
            - to: you@example.com
              send_resolved: true
```

```sh
helm upgrade --install observability ./helm/observability \
  --namespace observability -f obs-secrets.yaml --reuse-values
```

## Logs and traces

Every service logs JSON lines with `level`, `ts`, `msg` and `service`, and
whatever else is known: `schedule_id`, `client_id`, `provider`. A line logged
within a trace carries its `trace_id` and `span_id`, and Grafana links it to the
trace in Tempo; from a trace, it links back to the logs.

Kafka records carry the trace that produced them, so from the scheduler on, the
services continue each other's traces: the span of the tick that fired a
schedule is the parent of its send, and of any retries after it.
