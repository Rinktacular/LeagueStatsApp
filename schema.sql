-- ingestion runs (for monitoring & audit)
create table if not exists crawls (
  id bigserial primary key,
  region text not null,
  patch text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running',
  stats_json jsonb default '{}'::jsonb
);

-- de-dupe matches globally
create table if not exists seen_match_ids (
  match_id text primary key,
  region text not null,
  first_seen_at timestamptz not null default now()
);

-- queue table (optional if you want DB-backed queue)
create table if not exists match_queue (
  id bigserial primary key,
  match_id text unique,
  region text not null,
  enqueued_at timestamptz not null default now(),
  picked_at timestamptz,
  done_at timestamptz,
  status text not null default 'queued'
);

-- pointer to know where to resume per seed puuid
create table if not exists seed_progress (
  puuid text primary key,
  last_start_time bigint default null, -- unix seconds
  last_game_count int default 0,
  updated_at timestamptz not null default now()
);

create index if not exists idx_seen_region on seen_match_ids(region);
create index if not exists idx_mq_status on match_queue(status);
