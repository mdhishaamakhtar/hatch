# API

The API listens on http://localhost:9021. Its OpenAPI spec is generated from the
handlers' annotations (`make swag`) into [`swagger.yaml`](swagger.yaml) and
[`swagger.json`](swagger.json), and can be browsed at
http://localhost:9021/swagger/index.html.

Routes under `/v1` take a client's API key as a bearer token, and routes under
`/admin` take `ADMIN_API_KEY`. A client that exceeds its `max_rps` gets `429`,
with `Retry-After: 1`.

| Method | Path | Does |
|---|---|---|
| `POST` | `/v1/schedules` | Schedule an email |
| `GET` | `/v1/schedules/{id}` | Read one of the client's schedules |
| `DELETE` | `/v1/schedules/{id}` | Cancel a schedule that has not finished (`409` once it has) |
| `POST` | `/admin/clients` | Create a client. The response holds its API key, which is not stored and cannot be shown again |
| `DELETE` | `/admin/clients/{id}` | Deactivate a client: its key stops working, and its schedules are cancelled as they come due |
| `POST` | `/admin/clients/{id}/providers` | Register a client's credentials for a vendor, `mock` or `resend`, replacing any it had |
| `DELETE` | `/admin/clients/{id}/providers/{vendor}` | Remove a client's provider |

An error is a JSON object with an `error` code and, where it helps, a `reason`:
`{"error": "validation_failed", "reason": "deliver_at_too_soon"}`.

## Scheduling an email

```json
{
  "deliver_at": 1767225600000,
  "recipient_email": "someone@example.com",
  "from_email": "hello@yourdomain.com",
  "from_name": "Your Company",
  "subject": "Happy new year",
  "body": "<p>…</p>",
  "idempotency_key": "new-year-2026-someone",
  "metadata": {"campaign": "new-year"}
}
```

`deliver_at` is in Unix milliseconds. It has to be at least
`API_MIN_SCHEDULE_HORIZON` ahead (an hour, by default) and at most ten years.
The addresses must be bare addresses, without display names; `from_name` sets
the sender's name.

A client's `idempotency_key` names one schedule: sending the same key again
creates nothing, and answers `200` with the schedule the key created, instead of
`201`.
