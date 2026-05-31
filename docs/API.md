# API

The scheduler API is served on `http://localhost:9021`. The full OpenAPI spec is
generated from handler annotations (`make swag-gen`) and browsable at
**http://localhost:9021/swagger/index.html** (raw spec in
[`docs/swagger.yaml`](swagger.yaml) / [`docs/swagger.json`](swagger.json)).

Client routes under `/v1/*` require a client API key; admin routes under
`/admin/*` require `$ADMIN_API_KEY`. Per-client rate limits return `429` with
`Retry-After: 1` when exhausted.

## Timestamp format

`deliver_at` on every schedule request and response is an int64 of
milliseconds since the Unix epoch (UTC). Validation:

- `0` or missing → `deliver_at_required`
- negative → `deliver_at_format`
- less than 1 hour in the future → `deliver_at_too_soon`

## Endpoints at a glance

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/schedules` | Create a schedule (validation, idempotency key, active-provider check) |
| `GET` | `/v1/schedules/:id` | Fetch a schedule (client-scoped) |
| `DELETE` | `/v1/schedules/:id` | Cancel a schedule (status-guarded) |
| `POST` | `/admin/clients` | Create a client; returns the plaintext API key once |
| `DELETE` | `/admin/clients/:id` | Soft-delete a client (invalidates Redis cache) |
| `POST` | `/admin/clients/:id/providers` | Register/encrypt per-client provider credentials |
| `DELETE` | `/admin/clients/:id/providers/:vendor` | Soft-delete a provider |

See the Swagger UI for full request/response shapes.
