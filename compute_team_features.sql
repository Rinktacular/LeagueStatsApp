-- Neutral defaults: unknown champs count as BALANCED/FRONTLINE_0/ENGAGE_LOW
delete from team_features;

with tagged as (
  select
    p.match_id, p.team_id,
    coalesce(ct.dmg,       'MIXED')    as dmg,
    coalesce(ct.is_tank,   false)      as is_tank,
    coalesce(ct.has_engage,false)      as has_engage
  from participants p
  left join champ_tags ct on ct.champ_name = p.champ_name
)
insert into team_features (match_id, team_id, dmg_bucket, frontline_bucket, engage_bucket, mr_items_15, armor_items_15)
select
  match_id,
  team_id,
  case
    when avg((dmg='AP')::int) > 0.6 then 'AP_HEAVY'
    when avg((dmg='AD')::int) > 0.6 then 'AD_HEAVY'
    else 'BALANCED'
  end as dmg_bucket,
  case
    when sum((is_tank)::int) >= 2 then 'FRONTLINE_2+'
    when sum((is_tank)::int) = 1 then 'FRONTLINE_1'
    else 'FRONTLINE_0'
  end as frontline_bucket,
  case
    when sum((has_engage)::int) >= 2 then 'ENGAGE_HIGH'
    else 'ENGAGE_LOW'
  end as engage_bucket,
  null::int as mr_items_15,
  null::int as armor_items_15
from tagged
group by match_id, team_id;
