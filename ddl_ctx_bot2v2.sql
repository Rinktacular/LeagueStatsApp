drop materialized view if exists ctx_bot_2v2 cascade;

create materialized view ctx_bot_2v2 as
with base as (
  select
    m.patch,
    -- anchor on the BOTTOM (ADC) perspective for consistency
    case when p.team_position in ('BOTTOM','UTILITY') then 'BOTTOM' else p.team_position end as lane_anchor,
    p.champ_name                 as my_champ,
    -- my duo (ally) champion on bot lane
    case when p.team_position='BOTTOM' then ally_roles.supp_champ
         when p.team_position='UTILITY' then ally_roles.adc_champ
         else null end             as my_duo,
    -- enemy lane-opponent’s champ is already joined via opponents
    oppp.champ_name              as opp_lane_champ,
    -- enemy duo on bot
    case when p.team_position='BOTTOM' then enemy_roles.supp_champ
         when p.team_position='UTILITY' then enemy_roles.adc_champ
         else null end             as opp_duo,
    p.win::int                   as win_int,
    p.gd10::numeric              as gd10,
    p.xpd10::numeric             as xpd10,
    p.cs10::numeric              as cs10
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
  where p.team_position in ('BOTTOM','UTILITY') -- focus on bot lane 2v2
)
select
  patch, lane_anchor as team_position,
  my_champ as champ,
  opp_lane_champ as opponent,
  my_duo, opp_duo,
  count(*)                      as n,
  avg(win_int)::numeric         as winrate,
  avg(gd10)::numeric            as avg_gd10,
  avg(xpd10)::numeric           as avg_xpd10,
  avg(cs10)::numeric            as avg_cs10
from base
group by 1,2,3,4,5,6;

create index on ctx_bot_2v2 (champ, my_duo, opponent, opp_duo, patch);
