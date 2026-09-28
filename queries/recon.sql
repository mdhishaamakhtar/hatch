-- name: RecoverUnattempted :many
-- Rows no delivery attempt ever finished: pending past their deliver_at (the
-- scheduler never fired them), or processing for too long (the worker died
-- mid-send). Retry state is reset because no attempt counted.
--
-- The grace on pending rows matters: a row fired seconds ago stays pending
-- until a worker picks it up, and claiming those would only create duplicates.
UPDATE scheduled_emails
SET status = 'processing',
    retry_count = 0,
    last_provider = NULL,
    updated_at = now()
WHERE (status = 'pending' AND deliver_at < now() - interval '5 minutes')
   OR (status = 'processing' AND updated_at < now() - interval '10 minutes')
RETURNING id;

-- name: RecoverOrphanedRetries :many
-- Rows left retrying whose re-enqueue never happened. The failed attempt did
-- count, so retry_count and last_provider are kept.
UPDATE scheduled_emails
SET status = 'processing',
    updated_at = now()
WHERE status = 'retrying'
  AND updated_at < now() - interval '2 hours'
RETURNING id;
