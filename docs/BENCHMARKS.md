# Benchmarks

What Hatch can absorb, what stops it going faster, and which knob moves each
ceiling.

The numbers cited here all come from [`benchmarks/reference.md`](../benchmarks/reference.md),
which is written by the harness. Nothing in this file is typed by hand except
the reasoning. If the two ever disagree, the generated file is the measurement.

There are no targets in this document. Hatch does not promise a latency, and a
benchmark that grades itself against a number someone invented measures the
number, not the system. What follows is a capability description: at a given
shape and scale, this is what happens, and this is what would have to change to
make it happen faster.

## The question this answers

Not "how fast is one request". The useful question for a scheduler is a
*window* question:

> A company wants to send ten million emails between 8am and noon. Can this
> take it? What if they all come due at 9:00:00 exactly?

Those are two different questions, and Hatch answers them with two different
parts of itself. Getting the schedules *in* is the API's problem. Getting them
*out* on time is the wheel's and the workers'. They have separate ceilings, so
they are measured separately.

## The machine

Every absolute number below is relative to this host, and nothing else:

| | |
|---|---|
| Host | Apple M1 Pro, 10 cores, 16 GB |
| Cluster | Docker Desktop Kubernetes, 3 nodes |
| Provider | `MockProvider`, 150 ms latency + 0–50 ms jitter, 0.1% transient errors |

One thing to be clear about, because it caused a real failure during this run:
the three nodes report 10 CPUs *each*. They are three containers sharing one
10-core laptop. Kubernetes schedules against 30 phantom cores and will happily
pack pods until the host has nothing left. That is a property of this test rig,
not of Hatch, and it sets the top of the sweep.

## Running it

```sh
make bench-micro     # in-process, no cluster, seconds
make bench-all       # full sweep against the deployed stack, ~50 min
make bench SCENARIO=delivery COUNT=3000    # one point
```

`bench-all` runs as an in-cluster Job that talks to services over ClusterDNS.
Nothing depends on a port-forward surviving the run — an hour-long measurement
that dies because a tunnel dropped is not a measurement. The host side only
scales deployments between points and collects each Job's JSON.

Watch a run at <http://localhost:3000/d/hatch-benchmark>.

## Stage 1 — accepting schedules

| Concurrent clients | Accepted/sec | Client p50 | Server p95 |
|---|---|---|---|
| 8 | **1,706** | 1 ms | 10 ms |
| 32 | **1,591** | 7 ms | 87 ms |
| 128 | **1,741** | 89 ms | 93 ms |

All 5,000 schedules accepted at every point, no rate limiting, no errors.

Throughput is flat across a 16× range of client concurrency while p50 climbs
1 ms → 7 ms → 89 ms. That is the signature of a saturated server: it is already
at its ceiling by 8 clients, and every additional client converts directly into
queue time rather than work. **One API replica accepts roughly 1,700
schedules/sec.**

At that rate, loading a million schedules takes about ten minutes, and ten
million about an hour and a half. For a campaign planned in advance, ingest is
not the constraint — you can fill the schedule far faster than you can drain it.

> **To raise it:** the API is stateless behind a Service. Add replicas. The
> per-request work is one sha256 over the key, one Redis token-bucket call, and
> one Postgres insert; none of that serialises across replicas, so this should
> scale close to linearly until Postgres write throughput becomes the limit.

## Stage 2 — firing on time

The scheduler is not close to being a bottleneck and the numbers are almost
boring: producing a due schedule to Kafka has a p95 of **0.98 ms**.

Its real constraint is resolution, not rate. The wheel has 1-second slots, so a
schedule fires in the second it is due and never earlier — `SlotForDeliverAt`
rounds *up* precisely so a send cannot go out before the customer asked. Sub-
second precision is not on offer and would require changing the slot duration.

Work is split across scheduler pods by hashing the schedule id, so this stage
scales by adding pods to the StatefulSet.

## Stage 3 — sending

This is the ceiling that matters, and it has a simple shape:

```
emails/sec  ≈  replicas × send_concurrency ÷ provider_latency
```

| Topology | In flight | Sent/sec | Per hour |
|---|---|---|---|
| 4 replicas × concurrency 1 | 4 | **20.3** | 73k |
| 4 replicas × concurrency 8 | 32 | **122.0** | 439k |

The model predicts 22.8/sec and 183/sec. The first point matches almost
exactly; `processing` sat pinned at 4 for the entire drain, one in-flight send
per replica, exactly as configured. The second reaches about two thirds of
prediction — at 32 concurrent sends something other than provider latency has
started to bite. The next section is what.

**The provider is the pacer, not Hatch.** Each mock send sleeps 150–200 ms. A
worker doing one send at a time spends ~99% of its life blocked on I/O, which
is why `DELIVERY_SEND_CONCURRENCY` is the highest-leverage knob in the system:
going from 1 to 8 multiplied throughput by six.

> **To raise it:** raise `DELIVERY_SEND_CONCURRENCY` first — it costs nothing
> but goroutines and buys near-linear throughput while sends are I/O-bound.
> Then add replicas. Both are bounded by the two ceilings below.

## Where it actually stops

Two hard limits, both hit before the delivery model runs out of headroom.

**Postgres connections.** Each in-flight send holds roughly one connection —
peak `pg_stat_activity` was 26 with 4 sends in flight and 55 with 32, so about
one connection per additional concurrent send on top of a ~22-connection
baseline. `max_connections` is 100. That puts the hard bound at roughly **70
concurrent sends**, and it is why the sweep stops at 4 × 16 = 64.

**Host CPU.** Pushing past that during this run took the whole cluster down. It
is worth describing precisely, because the failure mode is not the obvious one:

At 4 replicas × 32 concurrency (128 in flight), throughput started at ~340/sec,
then collapsed to ~75/sec. `processing` never approached 128 — it thrashed
between 0 and 121. The benchmark's own status polling, a trivial `SELECT`,
started stalling for 6, 9, then 13 seconds at a time. Then pods began
restarting: api ×4, kafka ×3, both schedulers, one worker. Finally the
Kubernetes API server itself stopped answering.

Nothing was OOM-killed. Every restart was a graceful shutdown triggered by a
*failed liveness probe* — including `redis-cli ping` timing out after one
second. When a one-second ping cannot complete, the host is out of CPU, and
kubelet responds by killing healthy processes. The moment load stopped, the API
server answered on the first try.

So the collapse was not Hatch degrading. It was a laptop running out of cores,
and liveness probes converting a slowdown into an outage. That is a genuine
observation worth carrying into a real deployment — probe timeouts sized for an
idle system will amplify a load spike into a cascade — but it is not a
measurement of this software, so those points are excluded from the reference
set rather than reported as throughput.

> **To raise it:** more cores. Then `max_connections`, or a pgBouncer in front.
> On real hardware neither of these is where the interesting limit lives.

## Tuning the sweep for your machine

The sweep stops at 4 replicas × concurrency 16 because that is what a 10-core
laptop carries. That number is **hardcoded, not detected** — on a bigger machine
it will stop well short of your real ceiling, and on a smaller one it may still
choke. Both caps live in `cmd_all` in [`scripts/bench.sh`](../scripts/bench.sh),
in the two delivery loops.

The quantity to size is **in-flight sends** — `replicas × DELIVERY_SEND_CONCURRENCY`
— because that, not either factor alone, is what consumes the two scarce
resources.

### The database bound

Each in-flight send holds roughly one Postgres connection. Measure your own
budget rather than trusting that ratio:

```sh
kubectl -n hatch exec postgres-0 -- psql -U hatch -c "SHOW max_connections"
kubectl -n hatch exec postgres-0 -- psql -U hatch -c "SELECT count(*) FROM pg_stat_activity"
```

Run the second one with the stack idle to get your baseline. Then:

```
max_in_flight  ≈  (max_connections − idle_baseline) × 0.7
```

The 0.7 is headroom for the reconciliation and archival crons, the retry
consumer, and the benchmark's own observer connection — all of which want a
connection at the same moment the workers are busiest. On this host that is
`(100 − 22) × 0.7 ≈ 54`, and the sweep's 64 sits slightly above it, which is
consistent with the second delivery point reaching only two thirds of its
predicted throughput.

Every run reports `postgres_connections_peak` and `postgres_connections_max` in
its metrics, so you can check a point after the fact rather than guessing.

### The CPU bound

Harder, because the symptom appears only after you have caused it. Pick a
starting cap from cores — roughly **8 in-flight sends per physical core** worked
here — then run the sweep and watch for these, in this order:

```sh
kubectl -n hatch get pods                          # RESTARTS climbing
kubectl -n hatch get events | grep -i unhealthy    # probe timeouts
```

The signature is unmistakable once you know it: `Liveness probe failed: context
deadline exceeded`, restart reasons of `Completed` rather than `OOMKilled`, and
trivial commands like `redis-cli ping` timing out after one second. When a
one-second ping cannot complete, you are out of CPU, and kubelet is about to
start killing healthy pods. If the Kubernetes API server stops answering, you
are well past the edge — back the cap off and re-run; it recovers as soon as
load stops.

Note that a multi-node Docker Desktop or kind cluster reports each node's CPU
count as the **whole host's**. Three nodes on a 10-core laptop advertise 30
cores. Kubernetes will schedule against the phantom ones.

### Raising the ceilings rather than working around them

| Bound | Move it by |
|---|---|
| Postgres connections | raise `max_connections`, or put pgBouncer in front so connections stop tracking in-flight sends |
| Host CPU | more cores, or more nodes that are actually separate machines |
| Cascading restarts under load | size liveness probe timeouts for a *loaded* system, not an idle one |
| Provider latency | the model divides by it — a faster provider raises throughput at the same concurrency |

That last row is worth stating plainly: these numbers are all against
`MockProvider` at 150 ms + jitter, set by `MOCK_PROVIDER_LATENCY_MS`. Change it
and every delivery figure moves proportionally, because
`emails/sec ≈ in_flight ÷ provider_latency` is the whole model. Benchmarking
against a real provider measures that provider's rate limits instead, which is a
different and equally worthwhile question.

On a real cluster with a connection pooler and dedicated nodes, none of these
caps is likely to be the interesting limit. They are here because a laptop makes
them the interesting limit.

## The window question, answered

Using the measured 122 emails/sec on this laptop:

| Load | Shape | Outcome |
|---|---|---|
| 10k due in one second | spike | drains in ~82 s; last email ~82 s late |
| 100k due in one second | spike | drains in ~14 min |
| 1M due in one second | spike | drains in ~2.3 h |
| 1M spread over 4 h | ~69/sec | comfortable, ~57% utilised |
| 10M spread over 4 h | ~694/sec | not on this host — needs ~6× the send capacity |

Which is the honest answer to "can it take ten million between 8am and noon":
not on a laptop, and the arithmetic tells you exactly what it would take. The
model is linear in `replicas × concurrency`: 694/sec is about 120 in-flight
sends on paper, and since the measured throughput at 32 in flight came in at
two thirds of the model, budget closer to 180 — roughly 12 replicas at
concurrency 16, a connection pooler, and hardware with the cores to run them.

This is also why the punctuality numbers in the reference file look large
(p50 ~18 s). Every delivery point packs the whole batch into a *single* wheel
slot, so all 3,000 emails come due in the same instant and lateness is just
queue depth ÷ throughput. That is the spike row of the table above, measured.
It is not the system running late; it is the system draining a spike as fast as
it can. The `e2e` scenario, which spreads arrivals over 60 s the way real
traffic arrives, is the point where lateness means what it sounds like.

## Why the harness is worth trusting

A benchmark is only as good as its willingness to report bad news, so:

- **Ground truth comes from the rows, not the metrics.** Final counts are read
  straight from Postgres. A missed scrape or a counter reset cannot inflate them.
- **Throughput is the workers' own clock.** It is computed from the first and
  last terminal row, not from how long the load phase took, so a slow client
  cannot make the system look slow.
- **Every run declares whether it was valid.** The integrity checks assert that
  every schedule reached a terminal state and none was stranded. They are not
  performance targets — they say whether the run's numbers can be read at all.
- **It refuses to report numbers it cannot stand behind.** The load phase fails
  loudly with a status breakdown rather than silently measuring a smaller set,
  and a throughput figure computed over a window shorter than the arrival spread
  is marked invalid instead of published.

Two known measurement limits, stated rather than hidden:

- `provider_send_p95` reads 0.2425 s at every point. That is bucket
  interpolation, not a real signal — every send falls inside the single
  0.1–0.25 s histogram bucket, so the quantile is an artifact of bucket edges.
  Finer buckets would be needed to see contention here.
- Prometheus counters undercount slightly against the Postgres ground truth
  (e.g. 2,676 `sends_total` against 3,000 rows delivered), because a counter
  increase over a window does not align exactly with the run's boundaries. The
  row counts are authoritative.

## Why this is a good thing to benchmark

Most benchmark suites measure one number on one machine and stop. This one has
three stages with genuinely different bottlenecks — a CPU-bound HTTP path, a
timing-bound scheduler, and an I/O-bound fan-out — which makes it a real
exercise in finding *which* resource is binding rather than just turning a
crank. Both ceilings actually found here were discovered by measurement and
neither was the obvious guess: the delivery ceiling turned out to be database
connections rather than goroutines, and the collapse above it turned out to be
liveness probes rather than memory.

The two things it has already caught are the argument for keeping it:

- Authentication ran bcrypt on every request, capping ingest at **1.8/sec**.
  Replacing it with a sha256 lookup — correct for a 32-byte random key, which
  needs no work factor — moved that to **1,706/sec**.
- Delivery workers sent one email at a time. Adding bounded concurrency moved a
  single replica from **5.6/sec** to **146/sec**.

Neither was visible by reading the code. Both were obvious the moment something
measured them.
