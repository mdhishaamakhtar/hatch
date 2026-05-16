-- name: ReconPass1FreshAttempt :many
-- Stuck pending or processing rows: no real attempt was made yet. Reset retry
-- state, mark processing, return the (id, deliver_at) pairs so the cron can
-- re-enqueue to emails.due.
UPDATE scheduled_emails
SET status = 'processing',
    retry_count = 0,
    last_provider = NULL,
    updated_at = now()
WHERE (status = 'pending'    AND deliver_at < now())
   OR (status = 'processing' AND updated_at < now() - interval '10 minutes')
RETURNING id, deliver_at;

-- name: ReconPass2OrphanedRetry :many
-- Stuck retrying rows: an attempt did fail and burned a retry, so preserve
-- retry_count and last_provider. Mark processing and return for re-enqueue.
UPDATE scheduled_emails
SET status = 'processing',
    updated_at = now()
WHERE status = 'retrying'
  AND updated_at < now() - interval '2 hours'
RETURNING id, deliver_at;
