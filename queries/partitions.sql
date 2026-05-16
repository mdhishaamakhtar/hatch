-- name: CheckPartitionTerminal :one
-- Counts rows in the given partition that have not reached a terminal state.
-- The Partition Archival Cron uses this to decide whether a partition is
-- ready to be detached + exported + dropped. partition_name must be a known
-- attached partition of scheduled_emails.
SELECT count(*)::bigint AS non_terminal_count
FROM scheduled_emails
WHERE tableoid = @partition_name::regclass
  AND status NOT IN ('delivered', 'failed', 'cancelled');
