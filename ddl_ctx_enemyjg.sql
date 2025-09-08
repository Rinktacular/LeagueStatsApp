drop materialized view if exists ctx_lane_vs_enemyjg cascade;

create materialized view ctx_lane_vs_enemyjg as
with base as (
  select
    m.patch,
    p.team_position,
    p.champ_name               as champ,
    oppp.champ_name            as opponent,           -- your lane opponent’s champ
    enemy_roles.jungle_champ   as enemy_jungler,      -- enemy JG
    ally_roles.jungle_champ    as ally_jungler,       -- your JG (optional in queries)
    p.win::int                 as win_int,
    p.gd10::numeric            as gd10,
    p.xpd10::numeric           as xpd10,
    p.cs10::numeric            as cs10
  from opponents o
  join participants p
    on p.match_id=o.match_id and p.participant_id=o.your_participant_id
  join participants oppp
    on oppp.match_id=o.match_id and oppp.participant_id=o.opp_participant_id
  join matches m
    on m.match_id=o.match_id
  -- team role maps
  join team_role_map ally_roles
    on ally_roles.match_id=o.match_id and ally_roles.team_id = p.team_id
  join team_role_map enemy_roles
    on enemy_roles.match_id=o.match_id and enemy_roles.team_id = (case when p.team_id=100 then 200 else 100 end)
  where p.team_position <> 'JUNGLE' -- junglers don’t have a “lane opponent” in the same sense
)
select
  patch, team_position, champ, opponent, enemy_jungler, ally_jungler,
  count(*)                      as n,
  avg(win_int)::numeric         as winrate,
  avg(gd10)::numeric            as avg_gd10,
  avg(xpd10)::numeric           as avg_xpd10,
  avg(cs10)::numeric            as avg_cs10
from base
group by 1,2,3,4,5,6;

create index on ctx_lane_vs_enemyjg (team_position, champ, opponent, enemy_jungler, patch);
