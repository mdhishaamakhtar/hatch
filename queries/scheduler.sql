-- name: ListDue :many
-- The pending schedules in this pod's shard that fall due in (after, until].
SELECT id, deliver_at
FROM scheduled_emails
WHERE status = 'pending'
  AND deliver_at > sqlc.arg(after)
  AND deliver_at <= sqlc.arg(until)
  AND (hashtextextended(encode(id, 'hex'), 0) & 2147483647) % sqlc.arg(total_pods)::int = sqlc.arg(pod_index)::int;
