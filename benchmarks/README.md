# Hatch benchmarks

Two tiers. Tier 0 measures per-operation costs in-process; Tier 1/2 drives the
deployed stack. The tiers exist to support each other: a macro ceiling is only
defensible when a micro number explains it.

Results and analysis live in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

## Tier 0 — micro-benchmarks

```bash
make bench-micro
```

Ordinary Go benchmarks, living in the packages they measure. No cluster, no
setup, a few seconds to run. They cover the operations that turned out to set
the system's ceilings — the bcrypt compare in `ClientAuth`, the wheel's
`Append`/`Drain`/`Stats`, `Router.Select`, and `parseBatch`.

## Tier 1/2 — the deployed stack

```bash
make up-all                          # once
make bench-pf                        # port-forwards the harness reads through
make bench SCENARIO=ingest COUNT=200
make bench SCENARIO=delivery COUNT=400
make bench SCENARIO=e2e COUNT=400
```

Each run writes `benchmarks/reports/<scenario>-<timestamp>.{md,json}` and prints
the markdown to stdout. Watch it live at
<http://localhost:3000/d/hatch-benchmark> (admin/admin).

| Scenario | Question |
|---|---|
| `ingest` | How many schedules per second can the API accept, and what limits it? |
| `delivery` | How many emails per second can the delivery workers send? |
| `e2e` | Under a load the API can actually accept, does the pipeline hold the latency SLA? |

Knobs: `COUNT`, `WORKERS`, `SPREAD`, `LABEL`. Environment overrides are in
[`internal/bench/config.go`](internal/bench/config.go).

## Why it is built this way

**One stage at a time.** The three stages differ by more than an order of
magnitude, so a single blended "N per second" would only ever report the slowest
one and teach you nothing about why. Each scenario isolates one stage and
de-throttles the others — the `delivery` scenario deliberately runs with a lower
`BCRYPT_COST`, because ingest is not what it is measuring.

**Postgres is the ground truth.** Terminal-state counts come from the rows
themselves, never from a metric. A metric can be stale, reset, or never scraped;
and "every row reached a terminal state" is a stronger claim than "consumer lag
hit zero", which only says the messages were read. This also sidesteps the fact
that no Kafka exporter is deployed in this stack.

**Throughput comes from row timestamps, not `rate()`.** The span between the
first and last terminal write is the workers' real working window. It excludes
the load phase and the wait for maturity, and it is not smeared by the scrape
interval the way a `rate()` over a short window is.

**Load shape must not become the answer.** If `deliver_at` is spread over a
span, the observed window can never be shorter than that span — so a fast
pipeline silently reports `count / spread`, its own load shape, as a capacity.
Stage-ceiling runs therefore use `--spread 0`, and the harness refuses to let the
mistake pass quietly: when the worker window lands within 2× the spread, the
report prints `THROUGHPUT NOT VALID` and says to re-run. This guard exists
because the mistake was made, shipped, and had to be caught after the fact.

**The harness runs on the host.** Unlike `internal/verify`, the load generator
belongs *outside* the system under test, reaching the API through the same
LoadBalancer a real client would. Only the read-only observers need
port-forwards.

## Caveats that apply to every number here

- **Single laptop.** Postgres, Redis, Kafka, the six services, and the whole
  observability stack share 10 cores with the load generator. These are
  relative, comparative numbers, not capacity claims for real infrastructure.
- **Pod CPU limits are low and they bind.** The API is capped at 500m, which is
  most of the story behind its ingest ceiling.
- **`provider_send_duration_seconds` cannot resolve mock latency.** Its buckets
  jump `.1 → .25 → .5`, and every mock send (150–199ms) lands in one bucket, so
  `histogram_quantile` returns a constant. Read send latency from the mock
  configuration, not from that metric.
