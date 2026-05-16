CREATE TABLE clients (
    id              bytea       PRIMARY KEY,
    name            text        NOT NULL,
    api_key_hash    text        NOT NULL,
    is_active       boolean     NOT NULL DEFAULT true,
    max_rps         integer     NOT NULL DEFAULT 100,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX clients_api_key_hash_idx ON clients (api_key_hash);
