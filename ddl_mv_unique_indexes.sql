-- ddl_mv_unique_indexes.sql

-- enemy jg (no builds)
create unique index if not exists ux_ctx_lane_vs_enemyjg
on ctx_lane_vs_enemyjg (patch, team_position, champ, opponent, enemy_jungler, ally_jungler, skill_tier)
nulls not distinct;

-- enemy jg (builds)
create unique index if not exists ux_ctx_lane_vs_enemyjg_builds
on ctx_lane_vs_enemyjg_builds (patch, team_position, champ, opponent, enemy_jungler, ally_jungler,
                               mythic_id, boots_id, primary_keystone, secondary_style, skill_tier)
nulls not distinct;

-- bot 2v2 (builds)
create unique index if not exists ux_ctx_bot_2v2_builds
on ctx_bot_2v2_builds (patch, team_position, champ, my_duo, opponent, opp_duo,
                       mythic_id, boots_id, primary_keystone, secondary_style, skill_tier)
nulls not distinct;
