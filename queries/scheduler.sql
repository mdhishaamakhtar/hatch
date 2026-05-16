-- name: PollHourWindow :many
-- Returns the (id, deliver_at) pairs in this pod's hash slice that fall in
-- the next hour. Workers persist all delivery state in Postgres so the
-- scheduler only needs the trigger time.
SELECT id, deliver_at
FROM scheduled_emails
WHERE deliver_at BETWEEN now() AND now() + interval '1 hour'
  AND status = 'pending'
  AND ('x' || encode(substr(id, 1, 4), 'hex'))::bit(32)::int % @total_pods::int = @pod_index::int;
