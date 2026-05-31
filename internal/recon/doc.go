// Package recon holds the Phase 5 reconciliation-cron service: a periodic sweep
// that recovers schedule rows stranded by a crash and re-enqueues them onto
// emails.due for the delivery worker to pick up. It runs two SQL passes:
//
//   - Pass 1 (fresh attempt): rows stuck pending (deliver_at elapsed, never
//     fired) or processing (worker died mid-send). No real attempt was made, so
//     the SQL resets retry_count/last_provider before re-enqueuing.
//   - Pass 2 (orphaned retry): rows stuck retrying (a retry was initiated but the
//     retry consumer crashed before re-enqueuing). A retry was already burned, so
//     retry_count/last_provider are preserved.
//
// Re-enqueuing is idempotent by design — the delivery worker's Redis SET NX
// dedup means producing the same schedule_id twice never double-sends. See the
// LLD/HLD Reconciliation Cron for the full design.
package recon
