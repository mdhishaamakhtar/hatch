CREATE TYPE schedule_status AS ENUM (
    'pending',
    'processing',
    'retrying',
    'delivered',
    'failed',
    'cancelled'
);

-- Partitioned by month on deliver_at. A schedule's id is a UUIDv7 whose
-- timestamp is its deliver_at (see internal/db), so a lookup by id can always
-- filter on deliver_at too and touch one partition instead of all of them.
CREATE TABLE scheduled_emails (
    id               bytea           NOT NULL,
    client_id        bytea           NOT NULL REFERENCES clients (id),
    idempotency_key  text,
    deliver_at       timestamptz     NOT NULL,
    status           schedule_status NOT NULL DEFAULT 'pending',
    recipient_email  text            NOT NULL,
    from_email       text            NOT NULL,
    from_name        text,
    subject          text            NOT NULL,
    body             text            NOT NULL,
    metadata         jsonb,
    retry_count      smallint        NOT NULL DEFAULT 0,
    last_provider    text,
    failure_reason   text,
    created_at       timestamptz     NOT NULL DEFAULT now(),
    updated_at       timestamptz     NOT NULL DEFAULT now(),
    PRIMARY KEY (id, deliver_at)
) PARTITION BY RANGE (deliver_at);

-- The scheduler's poll and reconciliation's stuck-pending sweep.
CREATE INDEX scheduled_emails_status_deliver_at_idx ON scheduled_emails (status, deliver_at);
-- Reconciliation's stuck-processing and orphaned-retry sweeps.
CREATE INDEX scheduled_emails_status_updated_at_idx ON scheduled_emails (status, updated_at);

-- A unique constraint on a partitioned table must include the partition key,
-- so per-client idempotency keys are enforced in this side table instead.
CREATE TABLE schedule_idempotency (
    client_id        bytea       NOT NULL REFERENCES clients (id),
    idempotency_key  text        NOT NULL,
    schedule_id      bytea       NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, idempotency_key)
);
