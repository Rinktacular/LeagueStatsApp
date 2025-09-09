-- ddl_ctx_enemyjg_builds_with_cohort.sql

drop materialized view if exists ctx_lane_vs_enemyjg_builds;

create materialized view ctx_lane_vs_enemyjg_builds as
with base as (
  select
    m.patch,
    p.team_position,
    p.champ_name        as champ,
    oppp.champ_name     as opponent,

    -- Derive junglers from participants instead of team_role_map
    ally_jg.champ_name  as ally_jungler,
    enemy_jg.champ_name as enemy_jungler,

    cb.mythic_id, cb.boots_id,
    r.primary_keystone, r.secondary_style,

    tier_group(c.tier)  as skill_tier,

    p.win::int          as win_int,
    p.gd10::numeric     as gd10,
    p.xpd10::numeric    as xpd10,
    p.cs10::numeric     as cs10
  from opponents o
  join participants p
    on p.match_id = o.match_id and p.participant_id = o.your_participant_id
  join participants oppp
    on oppp.match_id = o.match_id and oppp.participant_id = o.opp_participant_id
  join matches m on m.match_id = o.match_id

  -- NEW: join participants again to get junglers by team
  join participants ally_jg
    on ally_jg.match_id = o.match_id
   and ally_jg.team_id  = p.team_id
   and ally_jg.team_position = 'JUNGLE'
  join participants enemy_jg
    on enemy_jg.match_id = o.match_id
   and enemy_jg.team_id  <> p.team_id
   and enemy_jg.team_position = 'JUNGLE'

  left join participant_core_build cb
    on cb.match_id = p.match_id and cb.participant_id = p.participant_id
  left join participant_runes r
    on r.match_id = p.match_id and r.participant_id = p.participant_id

  left join puuid_cohort_current c
    on c.puuid = p.puuid
)
select
  patch, team_position, champ, opponent,
  enemy_jungler, ally_jungler,
  mythic_id, boots_id, primary_keystone, secondary_style,
  skill_tier,
  count(*)::bigint      as n,
  avg(win_int)::numeric as winrate,
  avg(gd10)::numeric    as avg_gd10,
  avg(xpd10)::numeric   as avg_xpd10,
  avg(cs10)::numeric    as avg_cs10
from base
group by 1,2,3,4,5,6,7,8,9,10,11;

create index on ctx_lane_vs_enemyjg_builds (skill_tier, team_position, champ, opponent, enemy_jungler, ally_jungler, patch);
