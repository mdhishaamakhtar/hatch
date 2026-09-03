-- API keys are 32 random bytes generated server-side (see the admin create-client
-- handler), so the stored form only has to be irreversible — it does not need to
-- be slow. A deliberately slow hash exists to make guessing expensive, and there
-- is nothing to guess against a uniformly random 256-bit secret.
--
-- api_key_lookup is sha256(api_key). It serves as both the index the auth
-- middleware finds the client by and the credential check itself: presenting a
-- key whose digest matches the stored one is proof of holding the key.
CREATE TABLE clients (
    id              bytea       PRIMARY KEY,
    name            text        NOT NULL,
    api_key_lookup  bytea       NOT NULL,
    is_active       boolean     NOT NULL DEFAULT true,
    max_rps         integer     NOT NULL DEFAULT 100,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX clients_api_key_lookup_idx ON clients (api_key_lookup);
