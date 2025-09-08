-- =========================
-- 1) Helper: name normalizer
-- =========================
-- Immutable function (safe to index)
create or replace function norm_name(t text)
returns text language sql immutable as $$
  select case
           when t is null then null
           else regexp_replace(lower(trim(t)), '\s+', ' ', 'g')
         end
$$;

-- ======================================
-- 2) matches (used in many joins/filters)
-- ======================================
-- PK / natural keys
create index if not exists ix_matches_match_id on matches(match_id);
create index if not exists ix_matches_patch    on matches(patch);
create index if not exists ix_matches_queueid  on matches(queue_id);
create index if not exists ix_matches_ts       on matches(game_start_ts);

-- Common compound (patch + queue) for quick slicing
create index if not exists ix_matches_patch_queue on matches(patch, queue_id);

-- ==========================================
-- 3) participants (workhorse: heavy fan-out)
-- ==========================================
-- Cheap access by match + participant
create index if not exists ix_participants_match_part
  on participants(match_id, participant_id);

-- Speed lane/champ lookups (raw columns)
create index if not exists ix_participants_lane_champ
  on participants(team_position, champ_name);

-- Add normalized generated columns (non-destructive)
do $$
begin
  if not exists (
    select 1 from information_schema.columns
    where table_name = 'participants' and column_name = 'champ_norm'
  ) then
    alter table participants
      add column champ_norm text generated always as (norm_name(champ_name)) stored,
      add column opp_norm   text generated always as (norm_name(opponent_name)) stored;
  end if;
end$$;

-- Index normalized names for case/space-insensitive filtering
create index if not exists ix_participants_patch_lane_champnorm
  on participants(patch, team_position, champ_norm);
create index if not exists ix_participants_patch_lane_oppnorm
  on participants(patch, team_position, opp_norm);

-- Useful aggregates (patch / lane)
create index if not exists ix_participants_patch_lane
  on participants(patch, team_position);

-- Popular metrics by time (for debugging/time windows)
create index if not exists ix_participants_patch_ts
  on participants(patch, game_start_ts);

-- =================================
-- 4) opponents (pairings per match)
-- =================================
create index if not exists ix_opponents_match_part
  on opponents(match_id, your_participant_id);

create index if not exists ix_opponents_match_opp
  on opponents(match_id, opp_participant_id);

-- Quick “who fought whom” by match
create index if not exists ix_opponents_match_pair
  on opponents(match_id, your_participant_id, opp_participant_id);

-- =======================================
-- 5) participant_runes (joins by identity)
-- =======================================
create index if not exists ix_prunes_match_part
  on participant_runes(match_id, participant_id);

-- If you slice by keystone often
create index if not exists ix_prunes_keystone
  on participant_runes(primary_keystone);

-- =========================================
-- 6) participant_core_build (mythic/boots)
-- =========================================
create index if not exists ix_pbuild_match_part
  on participant_core_build(match_id, participant_id);

create index if not exists ix_pbuild_mythic_boots
  on participant_core_build(mythic_id, boots_id);

-- ==================================
-- 7) team_role_map (lane resolutions)
-- ==================================
create index if not exists ix_trm_match_team
  on team_role_map(match_id, team_id);

-- Helpful if you join by adc/supp frequently
create index if not exists ix_trm_roles
  on team_role_map(adc_champ, supp_champ);

-- =========================
-- 8) small maintenance ops
-- =========================
-- Analyze after creating lots of indexes so planner picks them up
analyze matches;
analyze participants;
analyze opponents;
analyze participant_runes;
analyze participant_core_build;
analyze team_role_map;
