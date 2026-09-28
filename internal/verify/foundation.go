package verify

import (
	"context"
	"time"

	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	// migrationVersion is the number of the last file in migrations/.
	migrationVersion = 4

	// topicPartitions is how many partitions each topic is created with.
	topicPartitions = 12
)

// checkFoundation checks what every service stands on: the database schema and
// the Kafka topics.
func (v *verifier) checkFoundation(ctx context.Context) {
	v.section("Foundation")

	var version int64
	var dirty bool
	if err := v.DB.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil {
		v.fail("read schema_migrations: %v", err)
	} else {
		v.check(version == migrationVersion && !dirty, "database migrated to version %d (dirty: %t)", version, dirty)
	}

	// The API takes schedules up to ten years out, so the partitions have to
	// run from this month to then.
	now := time.Now().UTC()
	for _, month := range []time.Time{now, now.AddDate(10, 0, 0)} {
		name := month.Format("scheduled_emails_y2006m01")
		v.check(v.tableExists(ctx, name), "partition %s exists", name)
	}

	topics := []string{kafka.TopicDue}
	for _, tier := range kafka.RetryTiers {
		topics = append(topics, tier.Topic)
	}
	req := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, t)
	}
	resp, err := req.RequestWith(ctx, v.producer)
	if err != nil {
		v.fail("read topic metadata: %v", err)
		return
	}
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			v.fail("topic %s: %v", *t.Topic, err)
			continue
		}
		v.check(len(t.Partitions) == topicPartitions, "topic %s has %d partitions", *t.Topic, len(t.Partitions))
	}
}

// tableExists reports whether a table named name exists.
func (v *verifier) tableExists(ctx context.Context, name string) bool {
	var exists bool
	err := v.DB.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists)
	return err == nil && exists
}
