-- API auth uses bcrypt for the credential check, but bcrypt hashes are
-- non-deterministic, so we can't look up a client row by hashing the
-- inbound key. We add a deterministic sha256 column (api_key_lookup) for the
-- index lookup, then verify with bcrypt against api_key_hash on hit.
--
-- The clients table has no rows yet (no client-creation flow shipped before
-- this migration), so we add the column as NOT NULL directly.
ALTER TABLE clients ADD COLUMN api_key_lookup bytea NOT NULL;
CREATE UNIQUE INDEX clients_api_key_lookup_idx ON clients (api_key_lookup);
DROP INDEX clients_api_key_hash_idx;
