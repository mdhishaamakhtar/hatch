-- credentials holds the Tink-encrypted provider credentials (see internal/crypto).
CREATE TABLE client_providers (
    id           bytea       PRIMARY KEY,
    client_id    bytea       NOT NULL REFERENCES clients (id),
    vendor       text        NOT NULL,
    credentials  bytea       NOT NULL,
    is_active    boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, vendor)
);
