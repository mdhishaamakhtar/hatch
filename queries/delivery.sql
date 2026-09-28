-- name: GetSchedules :many
-- deliver_ats must hold the deliver_at of every id: it is what lets Postgres
-- prune to the partitions involved instead of probing all of them.
SELECT *
FROM scheduled_emails
WHERE id = ANY(sqlc.arg(ids)::bytea[])
  AND deliver_at = ANY(sqlc.arg(deliver_ats)::timestamptz[]);

-- Every Mark* guards its transition with a status predicate and reports the rows
-- it changed, so terminal states are sticky: a duplicate emails.due record, or a
-- cancel that lands mid-send, changes nothing instead of rewriting the outcome.

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

-- name: IsClientActive :one
SELECT is_active
FROM clients
WHERE id = $1;

-- name: ListActiveProviders :many
SELECT vendor, credentials
FROM client_providers
WHERE client_id = $1
  AND is_active;
