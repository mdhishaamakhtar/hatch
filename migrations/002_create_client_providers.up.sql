CREATE TABLE client_providers (
    id           bytea       PRIMARY KEY,
    client_id    bytea       NOT NULL REFERENCES clients (id),
    vendor       text        NOT NULL,
    credentials  jsonb       NOT NULL,
    is_active    boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX client_providers_client_id_idx ON client_providers (client_id);
CREATE UNIQUE INDEX client_providers_client_vendor_idx ON client_providers (client_id, vendor);
