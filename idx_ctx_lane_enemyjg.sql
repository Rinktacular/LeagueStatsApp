-- 🔎 Primary lookup for lane + enemy JG context
-- Exact filters: patch, lane(team_position), champ, opponent, enemy_jungler
create index if not exists idx_ctx_lane_enemyjg_lookup
on ctx_lane_vs_enemyjg
  (patch, team_position, champ, opponent, enemy_jungler)
include (n, winrate, avg_gd10, avg_xpd10, avg_cs10);

-- 🧱 Builds variant: same filters + build keys
-- (lets index-only serve common selects when you also filter by mythic/boots/keystone)
create index if not exists idx_ctx_lane_enemyjg_builds_lookup
on ctx_lane_vs_enemyjg_builds
  (patch, team_position, champ, opponent, enemy_jungler, mythic_id, boots_id, primary_keystone)
include (n, winrate);

-- (Optional) If you frequently omit opponent but keep enemy jungler:
-- create index if not exists idx_ctx_lane_enemyjg_no_opp
-- on ctx_lane_vs_enemyjg (patch, team_position, champ, enemy_jungler);

-- (Optional) If you frequently omit enemy_jungler but keep opponent:
-- create index if not exists idx_ctx_lane_enemyjg_no_jg
-- on ctx_lane_vs_enemyjg (patch, team_position, champ, opponent);
