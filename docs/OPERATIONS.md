# Operations

How Hatch is built, deployed and checked. [The README](../README.md) covers
first-time setup, and [ARCHITECTURE.md](ARCHITECTURE.md) what each service does.
`make help` lists every target.

## Deploying

There are two Helm releases. `observability` (Prometheus, Grafana, Loki, Tempo)
is deployed once and left running; `hatch` (the services, and Postgres, Redis
and Kafka) is the one you iterate on.

| Command | What it does |
|---|---|
| `make up-all` | Deploy `observability`, then `hatch` |
| `make up` | Deploy `hatch` in three passes: the infrastructure, then the jobs that migrate the database and create the topics, then the services |
| `make build` | Build every service's image |
| `make up-pods` | Deploy the services again, with the images `make build` last built |
| `make down` / `make down-obs` / `make down-all` | Uninstall a release, keeping its volumes |
| `make reset` | Uninstall everything, delete the volumes, and deploy again |

A service changes in two steps: `make build-<service>` (for example
`make build-delivery-worker`), then `make up-pods`.

Every build gets a tag of its own, `hatch/<service>:dev-<unix time>`, written to
`.<service>-image-tag`, and `make up-pods` deploys the tags it finds there. A new
tag is what makes Kubernetes roll the pods over: rebuilding a fixed tag like
`:dev` would leave them running the old image.

## Configuration

`.env`, copied from `.env.example`, configures everything.
`scripts/inject-secrets.sh`, run by `make up`, loads it into the `hatch-secrets`
Secret, which every pod reads as its environment. The `HOST_*` keys are left out
of it: they point at `localhost`, for tools on your machine.

Each service's own settings, like its port or how often a cron sweeps, are set
by the chart, in `helm/hatch/values.yaml`; what they mean is in the service's
`Config` in `internal/`.

## Working on a service locally

`make port-forward` forwards Postgres, Redis and Kafka to localhost, and
`make run-<service>` runs a service against them with the `HOST_*` addresses
from `.env`: `make run-api`, for example. The scheduler runs as the only shard,
with its wheel in `.local/`.

| Command | What it does |
|---|---|
| `make test` | Run the tests |
| `make sqlc` | Regenerate `internal/db` after changing `queries/` or `migrations/` |
| `make swag` | Regenerate the OpenAPI spec in `docs/` after changing the API |
| `make migrate` / `make migrate-down` | Apply or roll back the migrations on the forwarded database. `make up` migrates the cluster's database itself |
| `make status` | Show the pods |
| `make logs SVC=<component>` | Follow a component's logs |
| `make gen-provider-key` | Print a new key for `PROVIDER_CRED_KEY` |

The migrations can be edited in place: nothing depends on their history. After
changing one, `make reset` recreates the database from scratch.

## `make verify`

`make verify` is the acceptance check for a deployed stack. It builds, vets and
tests the code, checks `internal/db` matches the queries, and checks every pod is
running. Then it runs `cmd/verify` as a Job in the cluster, which takes the stack
through each stage in turn, from the API to the archival cron and the telemetry
they emit, printing a `[PASS]` or `[FAIL]` line per check.

One of those checks sends a real email through Resend, so it needs
`VERIFY_RESEND_API_KEY` in `.env`, and a sending domain verified in Resend.
