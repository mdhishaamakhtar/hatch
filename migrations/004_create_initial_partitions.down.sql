-- Nothing to do: dropping scheduled_emails in 003's down migration drops its
-- partitions. Dropping 1200 tables here in one transaction would exhaust
-- max_locks_per_transaction.
SELECT 1;
