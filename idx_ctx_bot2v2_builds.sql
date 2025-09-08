-- Strong lookup index for exact bot 2v2 queries
-- (patch + champ/my_duo/opponent/opp_duo), includes metrics for index-only scans
create index if not exists idx_ctx_bot2v2_builds_lookup
on ctx_bot_2v2_builds
  (patch, champ, my_duo, opponent, opp_duo)
include (team_position, n, winrate, avg_gd10, avg_xpd10, avg_cs10);

-- Broader fallback when you only know "my side" (patch + champ + my_duo)
create index if not exists idx_ctx_bot2v2_builds_myside
on ctx_bot_2v2_builds
  (patch, champ, my_duo);

-- Optional: if you sometimes filter by team_position (BOTTOM) explicitly
create index if not exists idx_ctx_bot2v2_builds_lane_quad
on ctx_bot_2v2_builds
  (patch, team_position, champ, my_duo, opponent, opp_duo);
