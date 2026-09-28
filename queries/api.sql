-- name: GetClientByAPIKey :one
-- Authenticates a request. Whether the client can send at all rides along, so
-- creating a schedule needs no extra round trip to check it.
SELECT id, max_rps,
       EXISTS (SELECT 1 FROM client_providers p WHERE p.client_id = clients.id AND p.is_active) AS has_provider
FROM clients
WHERE api_key_lookup = $1
  AND is_active;

-- name: CreateSchedule :one
INSERT INTO scheduled_emails (
    id, client_id, deliver_at, recipient_email, from_email, from_name, subject, body, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING status;

-- name: CreateScheduleWithKey :one
-- Claims the idempotency key and inserts the schedule in one statement, so the
-- two can never disagree. When the key is already taken nothing is inserted and
-- no row comes back.
WITH claimed AS (
    INSERT INTO schedule_idempotency (client_id, idempotency_key, schedule_id)
    VALUES (sqlc.arg(client_id), sqlc.arg(idempotency_key), sqlc.arg(id))
    ON CONFLICT DO NOTHING
    RETURNING schedule_id
)
INSERT INTO scheduled_emails (
    id, client_id, idempotency_key, deliver_at, recipient_email, from_email, from_name, subject, body, metadata
)
SELECT schedule_id,
       sqlc.arg(client_id)::bytea,
       sqlc.arg(idempotency_key)::text,
       sqlc.arg(deliver_at)::timestamptz,
       sqlc.arg(recipient_email)::text,
       sqlc.arg(from_email)::text,
       sqlc.narg(from_name)::text,
       sqlc.arg(subject)::text,
       sqlc.arg(body)::text,
       sqlc.narg(metadata)::jsonb
FROM claimed
RETURNING status;

-- name: GetScheduleIDByKey :one
SELECT schedule_id
FROM schedule_idempotency
WHERE client_id = $1
  AND idempotency_key = $2;

-- name: GetSchedule :one
SELECT *
FROM scheduled_emails
WHERE id = $1
  AND deliver_at = $2
  AND client_id = $3;

-- name: CancelSchedule :execrows
UPDATE scheduled_emails
SET status = 'cancelled',
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND client_id = $3
  AND status IN ('pending', 'processing', 'retrying');

-- name: CreateClient :exec
INSERT INTO clients (id, name, api_key_lookup, max_rps)
VALUES ($1, $2, $3, $4);

-- name: DeactivateClient :exec
UPDATE clients
SET is_active = false
WHERE id = $1;

-- name: UpsertClientProvider :one
INSERT INTO client_providers (id, client_id, vendor, credentials)
VALUES ($1, $2, $3, $4)
ON CONFLICT (client_id, vendor) DO UPDATE
SET credentials = EXCLUDED.credentials,
    is_active = true
RETURNING id;

-- name: DeactivateClientProvider :exec
UPDATE client_providers
SET is_active = false
WHERE client_id = $1
  AND vendor = $2;
