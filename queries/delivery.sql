-- name: BatchFetchSchedules :many
SELECT *
FROM scheduled_emails
WHERE id = ANY(@ids::bytea[]);

-- Every MarkX below guards the transition with a status predicate and returns
-- its row count (:execrows), so terminal states are sticky: a duplicate
-- emails.due record for a delivered/failed/cancelled row, or a cancel that
-- landed mid-send, changes 0 rows instead of silently rewriting the outcome.
-- The caller treats 0 rows as "state moved under me — log and stop".

-- name: MarkProcessing :execrows
UPDATE scheduled_emails
SET status = 'processing',
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND status IN ('pending', 'processing', 'retrying');

-- name: MarkDelivered :execrows
UPDATE scheduled_emails
SET status = 'delivered',
    last_provider = $3,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND status = 'processing';

-- name: MarkRetrying :execrows
UPDATE scheduled_emails
SET status = 'retrying',
    retry_count = retry_count + 1,
    last_provider = $3,
    failure_reason = $4,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND status = 'processing';

-- name: MarkFailed :execrows
UPDATE scheduled_emails
SET status = 'failed',
    last_provider = $3,
    failure_reason = $4,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND status = 'processing';

-- name: MarkCancelled :execrows
UPDATE scheduled_emails
SET status = 'cancelled',
    failure_reason = $3,
    updated_at = now()
WHERE id = $1
  AND deliver_at = $2
  AND status = 'processing';

-- name: GetClientForDelivery :one
SELECT is_active
FROM clients
WHERE id = $1;
