// Package archival holds the Phase 5 partition-archival service: a periodic
// lifecycle sweep over the scheduled_emails partitions. For each partition whose
// month is fully in the past, it archives the partition iff every row is in a
// terminal state (delivered/failed/cancelled): detach from the parent, export to
// a gzip CSV under the archive directory, then drop the detached table to reclaim
// disk.
//
// The sweep is lightweight and idempotent. The 1200 monthly partitions are
// pre-created with a 100-year forward runway (migration 004) and this service
// only ever drops fully-past, fully-terminal partitions — it never creates them,
// and it never touches the current/future runway. A partition with non-terminal
// rows is left in place and retried next cycle. See the LLD/HLD Partition
// Archival Cron for the full design.
package archival
