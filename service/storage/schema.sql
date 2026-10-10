CREATE TABLE IF NOT EXISTS tenants (
  id text PRIMARY KEY,
  generation bigint NOT NULL DEFAULT 1,
  account_counter bigint NOT NULL DEFAULT 0,
  operation_counter bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS accounts (
  tenant text NOT NULL REFERENCES tenants(id),
  generation bigint NOT NULL,
  id text NOT NULL,
  name text NOT NULL,
  balance bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant, generation, id)
);
CREATE TABLE IF NOT EXISTS operations (
  request_id uuid PRIMARY KEY,
  tenant text NOT NULL REFERENCES tenants(id),
  generation bigint NOT NULL,
  request_key text NOT NULL,
  fingerprint text NOT NULL,
  command text NOT NULL,
  args text[] NOT NULL,
  amount bigint NOT NULL DEFAULT 0,
  public_id text,
  status text NOT NULL DEFAULT 'received' CHECK (status IN ('received','completed','failed','cancelled')),
  http_status integer,
  response jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  UNIQUE (tenant, generation, request_key),
  UNIQUE (tenant, generation, public_id)
);
CREATE TABLE IF NOT EXISTS outbox (
  request_id uuid PRIMARY KEY REFERENCES operations(request_id) ON DELETE CASCADE,
  partition_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS ledger_entries (
  request_id uuid NOT NULL REFERENCES operations(request_id) ON DELETE CASCADE,
  account_id text NOT NULL,
  delta bigint NOT NULL,
  PRIMARY KEY (request_id, account_id)
);
CREATE OR REPLACE FUNCTION check_ledger_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM ledger_entries WHERE request_id = NEW.request_id
             GROUP BY request_id HAVING SUM(delta) <> 0) THEN
    RAISE EXCEPTION 'unbalanced ledger operation';
  END IF;
  RETURN NULL;
END; $$;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ledger_balanced' AND tgrelid = 'ledger_entries'::regclass) THEN
    CREATE CONSTRAINT TRIGGER ledger_balanced AFTER INSERT ON ledger_entries
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_ledger_balanced();
  END IF;
END; $$;
CREATE TABLE IF NOT EXISTS shard_leases (
  shard_id text PRIMARY KEY,
  owner text NOT NULL,
  epoch bigint NOT NULL DEFAULT 1,
  expires_at timestamptz NOT NULL,
  checkpoint text NOT NULL DEFAULT '',
  drained boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS dead_letters (
  shard_id text NOT NULL,
  sequence_number text NOT NULL,
  payload bytea NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (shard_id, sequence_number)
);
