// Package retry holds the Phase 4 retry-consumer service: one logical consumer
// per retry tier (emails.retry.1min / 5min / 30min) that drains its topic on a
// schedule and re-enqueues each schedule_id back to emails.due with a fresh
// OTel context. There is no retry logic here — the delivery worker decides
// exhaustion on re-attempt from the Postgres retry_count, so the consumer is
// stateless (Kafka holds the messages; no Postgres or Redis). See the LLD/HLD
// Retry Consumers for the full design.
package retry
