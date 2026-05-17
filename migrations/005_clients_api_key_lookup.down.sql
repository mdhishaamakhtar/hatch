CREATE UNIQUE INDEX clients_api_key_hash_idx ON clients (api_key_hash);
DROP INDEX IF EXISTS clients_api_key_lookup_idx;
ALTER TABLE clients DROP COLUMN api_key_lookup;
