-- ddl_cohorts_bootstrap.sql

-- Optional: ensure participants.puuid exists (no-op if already there)
alter table if exists participants
  add column if not exists puuid text;

create table if not exists puuid_cohort_current (
  puuid       text primary key,
  platform    text not null,  -- na1/euw1/kr (platform host)
  tier        text not null,  -- IRON..CHALLENGER
  division    text null,      -- I..IV for sub-Master
  updated_at  timestamptz not null default now()
);

create index if not exists idx_puuid_cohort_platform on puuid_cohort_current(platform);
create index if not exists idx_puuid_cohort_tier     on puuid_cohort_current(tier);

-- Normalize Riot tiers to a single group we can filter on easily.
create or replace function tier_group(tier text) returns text
language sql immutable as $$
  select case upper(coalesce(tier, ''))
    when 'CHALLENGER' then 'MASTER_PLUS'
    when 'GRANDMASTER' then 'MASTER_PLUS'
    when 'MASTER' then 'MASTER_PLUS'
    when 'DIAMOND' then 'DIAMOND'
    when 'EMERALD' then 'EMERALD'
    when 'PLATINUM' then 'PLATINUM'
    when 'GOLD' then 'GOLD'
    when 'SILVER' then 'SILVER'
    when 'BRONZE' then 'BRONZE'
    when 'IRON' then 'IRON'
    else 'UNRANKED'
  end
$$;
