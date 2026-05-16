CREATE TYPE schedule_status AS ENUM (
    'pending',
    'processing',
    'retrying',
    'delivered',
    'failed',
    'cancelled'
);

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

-- Partitioned-table limitation: a UNIQUE constraint must include every
-- partition key column. Since dedup is by (client_id, idempotency_key) only,
-- we enforce it in a side table that points at the owning schedule row.
CREATE TABLE schedule_idempotency (
    client_id        bytea       NOT NULL REFERENCES clients (id),
    idempotency_key  text        NOT NULL,
    schedule_id      bytea       NOT NULL,
    deliver_at       timestamptz NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, idempotency_key)
);

CREATE INDEX scheduled_emails_deliver_status_idx
    ON scheduled_emails (deliver_at, status);

CREATE INDEX scheduled_emails_status_updated_idx
    ON scheduled_emails (status, updated_at);
