drop materialized view if exists lane_matchup_stats;

create materialized view lane_matchup_stats as
with pairs as (
  select
      m.patch,
      o.team_position,
      p.champ_name  as champ,
      opp.champ_name as opponent,
      p.win::int     as win_int,
      p.kills, p.deaths, p.assists,
      coalesce(p.cs10,  0)::numeric as cs10,
      coalesce(p.xpd10, 0)::numeric as xpd10,
      coalesce(p.gd10,  0)::numeric as gd10
  from opponents o
  join participants p
    on p.match_id = o.match_id
   and p.participant_id = o.your_participant_id
  join participants opp
    on opp.match_id = o.match_id
   and opp.participant_id = o.opp_participant_id
  join matches m
    on m.match_id = o.match_id
)
select
  patch,
  team_position,
  champ,
  opponent,
  count(*)                                as n,
  avg(win_int)::numeric                   as winrate,        -- 0.0–1.0
  avg(gd10)::numeric                      as avg_gd10,
  avg(xpd10)::numeric                     as avg_xpd10,
  avg(cs10)::numeric                      as avg_cs10,
  avg(kills)::numeric                     as avg_kills,
  avg(deaths)::numeric                    as avg_deaths,
  avg(assists)::numeric                   as avg_assists,
  stddev_pop(gd10)::numeric               as sd_gd10,
  stddev_pop(xpd10)::numeric              as sd_xpd10,
  stddev_pop(cs10)::numeric               as sd_cs10
from pairs
group by patch, team_position, champ, opponent;

-- helpful index for API queries
create index idx_lms_lookup on lane_matchup_stats(team_position, champ, opponent, patch);
