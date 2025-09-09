-- ddl_ctx_bot2v2_builds_with_cohort.sql

drop materialized view if exists ctx_bot_2v2_builds;

create materialized view ctx_bot_2v2_builds as
with base as (
  select
    m.patch,
    case when p.team_position in ('BOTTOM','UTILITY') then 'BOTTOM' else p.team_position end as team_position,
    p.champ_name as champ,
    case when p.team_position='BOTTOM' then ally_roles.supp_champ
         when p.team_position='UTILITY' then ally_roles.adc_champ
         else null end as my_duo,
    oppp.champ_name as opponent,
    case when p.team_position='BOTTOM' then enemy_roles.supp_champ
         when p.team_position='UTILITY' then enemy_roles.adc_champ
         else null end as opp_duo,
    cb.mythic_id, cb.boots_id,
    r.primary_keystone, r.secondary_style,

    -- NEW: bucketed skill cohort from the player's latest visible rank
    tier_group(c.tier) as skill_tier,

    p.win::int       as win_int,
    p.gd10::numeric  as gd10,
    p.xpd10::numeric as xpd10,
    p.cs10::numeric  as cs10
  from opponents o
  join participants p
    on p.match_id=o.match_id and p.participant_id=o.your_participant_id
  join participants oppp
    on oppp.match_id=o.match_id and oppp.participant_id=o.opp_participant_id
  join matches m on m.match_id=o.match_id
  join team_role_map ally_roles
    on ally_roles.match_id=o.match_id and ally_roles.team_id = p.team_id
  join team_role_map enemy_roles
    on enemy_roles.match_id=o.match_id and enemy_roles.team_id = (case when p.team_id=100 then 200 else 100 end)
  left join participant_core_build cb
    on cb.match_id=p.match_id and cb.participant_id=p.participant_id
  left join participant_runes r
    on r.match_id=p.match_id and r.participant_id=p.participant_id

  -- NEW: attach latest tier snapshot
  left join puuid_cohort_current c
    on c.puuid = p.puuid

  where p.team_position in ('BOTTOM','UTILITY')
)
select
  patch, team_position, champ, my_duo, opponent, opp_duo,
  mythic_id, boots_id, primary_keystone, secondary_style,
  skill_tier,                                -- NEW
  count(*)::bigint        as n,
  avg(win_int)::numeric   as winrate,
  avg(gd10)::numeric      as avg_gd10,
  avg(xpd10)::numeric     as avg_xpd10,
  avg(cs10)::numeric      as avg_cs10
from base
group by 1,2,3,4,5,6,7,8,9,10,11;

create index on ctx_bot_2v2_builds (skill_tier, team_position, champ, my_duo, opponent, opp_duo, patch);
