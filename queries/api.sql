-- name: CreateSchedule :one
INSERT INTO scheduled_emails (
    id, client_id, idempotency_key, deliver_at,
    recipient_email, from_email, from_name, subject, body, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
RETURNING id, status, deliver_at, created_at;

-- name: GetScheduleByID :one
SELECT *
FROM scheduled_emails
WHERE id = $1
  AND client_id = $2
LIMIT 1;

-- name: GetScheduleByIdempotencyKey :one
SELECT *
FROM scheduled_emails
WHERE client_id = $1
  AND idempotency_key = $2
LIMIT 1;

-- name: CancelSchedule :one
UPDATE scheduled_emails
SET status = 'cancelled',
    updated_at = now()
WHERE id = $1
  AND client_id = $2
  AND status IN ('pending', 'processing', 'retrying')
RETURNING id, status, updated_at;

-- name: CreateClient :one
INSERT INTO clients (id, name, api_key_hash, max_rps)
VALUES ($1, $2, $3, $4)
RETURNING id, name, max_rps, is_active, created_at;

-- name: SoftDeleteClient :exec
UPDATE clients
SET is_active = false
WHERE id = $1;

-- name: GetClientByAPIKeyHash :one
SELECT id, name, max_rps, is_active
FROM clients
WHERE api_key_hash = $1
  AND is_active = true
LIMIT 1;

-- name: UpsertClientProvider :one
INSERT INTO client_providers (id, client_id, vendor, credentials, is_active)
VALUES ($1, $2, $3, $4, true)
ON CONFLICT (client_id, vendor) DO UPDATE
SET credentials = EXCLUDED.credentials,
    is_active = true
RETURNING id, client_id, vendor, is_active;

-- name: SoftDeleteClientProvider :exec
UPDATE client_providers
SET is_active = false
WHERE client_id = $1
  AND vendor = $2;

-- name: ListClientActiveProviders :many
SELECT id, vendor, credentials
FROM client_providers
WHERE client_id = $1
  AND is_active = true;
