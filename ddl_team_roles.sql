drop view if exists team_role_map cascade;

create view team_role_map as
with r as (
  select
    match_id, team_id,
    max(champ_name) filter (where team_position='TOP')     as top_champ,
    max(champ_name) filter (where team_position='JUNGLE')  as jungle_champ,
    max(champ_name) filter (where team_position='MIDDLE')  as mid_champ,
    max(champ_name) filter (where team_position='BOTTOM')  as adc_champ,
    max(champ_name) filter (where team_position='UTILITY') as supp_champ
  from participants
  group by match_id, team_id
)
select * from r;
