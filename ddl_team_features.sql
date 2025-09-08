create table if not exists team_features (
  match_id text not null,
  team_id smallint not null,             -- 100|200
  dmg_bucket text,                       -- AD_HEAVY|AP_HEAVY|BALANCED
  frontline_bucket text,                 -- FRONTLINE_0|1|2+
  engage_bucket text,                    -- ENGAGE_LOW|HIGH
  mr_items_15 int,                       -- (optional, not filled yet)
  armor_items_15 int,
  primary key (match_id, team_id)
);
