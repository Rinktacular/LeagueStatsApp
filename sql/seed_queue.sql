BEGIN;
CREATE TABLE IF NOT EXISTS lol.seed_queue (
  puuid       TEXT PRIMARY KEY,
  region_routing TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'PENDING',
  last_error  TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS seed_queue_status_idx ON lol.seed_queue (status);
COMMIT;
