-- 1) Helper: name normalizer (no backslashes; uses POSIX class)
create or replace function norm_name(t text)
returns text language sql immutable as $$
  select case
           when t is null then null
           else regexp_replace(lower(btrim(t)), '[[:space:]]+', ' ', 'g')
         end
$$;

-- 2) matches (only columns we know exist)
create index if not exists ix_matches_match_id on matches(match_id);
create index if not exists ix_matches_patch    on matches(patch);

-- 3) participants
create index if not exists ix_participants_match_part
  on participants(match_id, participant_id);

create index if not exists ix_participants_lane_champ
  on participants(team_position, champ_name);

-- normalized champ name (no DO block; IF NOT EXISTS)
alter table participants
  add column if not exists champ_norm text
  generated always as (norm_name(champ_name)) stored;

create index if not exists ix_participants_match_lane_champnorm
  on participants(match_id, team_position, champ_norm);

create index if not exists ix_participants_match_lane
  on participants(match_id, team_position);

-- 4) opponents
create index if not exists ix_opponents_match_part
  on opponents(match_id, your_participant_id);

create index if not exists ix_opponents_match_opp
  on opponents(match_id, opp_participant_id);

create index if not exists ix_opponents_match_pair
  on opponents(match_id, your_participant_id, opp_participant_id);

-- 5) participant_runes
create index if not exists ix_prunes_match_part
  on participant_runes(match_id, participant_id);

create index if not exists ix_prunes_keystone
  on participant_runes(primary_keystone);

-- 6) participant_core_build
create index if not exists ix_pbuild_match_part
  on participant_core_build(match_id, participant_id);

create index if not exists ix_pbuild_mythic_boots
  on participant_core_build(mythic_id, boots_id);

-- 7) Analyze
analyze matches;
analyze participants;
analyze opponents;
analyze participant_runes;
analyze participant_core_build;
