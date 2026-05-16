-- No-op: partitions are owned by the parent scheduled_emails table, which the
-- 003 down-migration drops with cascading effect. Trying to drop 1200
-- partition tables here in a single transaction exhausts Postgres's per-txn
-- lock table (max_locks_per_transaction).
SELECT 1;
