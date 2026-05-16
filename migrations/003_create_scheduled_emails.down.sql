DROP INDEX IF EXISTS scheduled_emails_status_updated_idx;
DROP INDEX IF EXISTS scheduled_emails_deliver_status_idx;
DROP TABLE IF EXISTS schedule_idempotency;
DROP TABLE IF EXISTS scheduled_emails;
DROP TYPE IF EXISTS schedule_status;
