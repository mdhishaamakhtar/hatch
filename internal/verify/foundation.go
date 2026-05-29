package verify

import (
	"context"
	"fmt"
)

// expectedMigrationVersion is the highest golang-migrate version applied; bump
// this when a new migration lands.
const expectedMigrationVersion = 5

// expectedPartitions is the number of partitions attached to scheduled_emails
// by migration 004.
const expectedPartitions = 1200

// checkFoundation asserts the database is migrated and partitioned, querying
// Postgres directly over ClusterDNS (replacing the host `migrate version` /
// `psql` calls).
func (v *Verifier) checkFoundation(ctx context.Context) {
	v.rep.Section("Foundation — migrations + partitions")

	var version int64
	var dirty bool
	err := v.pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty)
	switch {
	case err != nil:
		v.rep.Failf("read schema_migrations: %v", err)
	case dirty:
		v.rep.Failf("schema_migrations is dirty at version %d", version)
	case version != expectedMigrationVersion:
		v.rep.Failf("migrate version = %d, want %d", version, expectedMigrationVersion)
	default:
		v.rep.Passf("migrate at version %d (all migrations applied, not dirty)", version)
	}

	var parts int
	err = v.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'scheduled_emails'::regclass`).Scan(&parts)
	if err != nil {
		v.rep.Failf("count partitions: %v", err)
		return
	}
	v.rep.Check(parts == expectedPartitions,
		fmt.Sprintf("%d partitions attached to scheduled_emails", parts),
		fmt.Sprintf("partition count = %d, want %d", parts, expectedPartitions))
}
