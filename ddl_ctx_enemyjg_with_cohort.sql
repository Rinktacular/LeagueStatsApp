-- ddl_ctx_enemyjg_with_cohort.sql

drop materialized view if exists ctx_lane_vs_enemyjg;

create materialized view ctx_lane_vs_enemyjg as
with base as (
  select
    m.patch,
    p.team_position,
    p.champ_name        as champ,
    oppp.champ_name     as opponent,

    ally_jg.champ_name  as ally_jungler,
    enemy_jg.champ_name as enemy_jungler,

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

  join participants ally_jg
    on ally_jg.match_id = o.match_id
   and ally_jg.team_id  = p.team_id
   and ally_jg.team_position = 'JUNGLE'
  join participants enemy_jg
    on enemy_jg.match_id = o.match_id
   and enemy_jg.team_id  <> p.team_id
   and enemy_jg.team_position = 'JUNGLE'

  left join puuid_cohort_current c
    on c.puuid = p.puuid
)
select
  patch, team_position, champ, opponent,
  enemy_jungler, ally_jungler,
  skill_tier,
  count(*)::bigint      as n,
  avg(win_int)::numeric as winrate,
  avg(gd10)::numeric    as avg_gd10,
  avg(xpd10)::numeric   as avg_xpd10,
  avg(cs10)::numeric    as avg_cs10
from base
group by 1,2,3,4,5,6,7;

create index on ctx_lane_vs_enemyjg (skill_tier, team_position, champ, opponent, enemy_jungler, ally_jungler, patch);
