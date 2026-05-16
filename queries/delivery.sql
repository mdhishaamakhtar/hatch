-- name: BatchFetchSchedules :many
SELECT *
FROM scheduled_emails
WHERE id = ANY(@ids::bytea[]);

-- name: MarkProcessing :exec
UPDATE scheduled_emails
SET status = 'processing',
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2;

-- name: MarkDelivered :exec
UPDATE scheduled_emails
SET status = 'delivered',
    last_provider = $3,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2;

-- name: MarkRetrying :exec
UPDATE scheduled_emails
SET status = 'retrying',
    retry_count = retry_count + 1,
    last_provider = $3,
    failure_reason = $4,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2;

-- name: MarkFailed :exec
UPDATE scheduled_emails
SET status = 'failed',
    last_provider = $3,
    failure_reason = $4,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2;

-- name: MarkCancelled :exec
UPDATE scheduled_emails
SET status = 'cancelled',
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2;
